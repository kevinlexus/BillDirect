package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * Сервис выполнения процессов формирования
 *
 * @author lev
 * @version 1.12
 */
@Slf4j
@Service
@Scope("prototype")
public class ProcessMngImpl implements ProcessMng, CommonConstants {

    private final ConfigApp config;
    private final ThreadMng<Long> threadMng;
    private final GenChrgProcessMng genChrgProcessMng;
    private final GenPenProcessMng genPenProcessMng;
    private final MigrateMng migrateMng;
    private final DistVolMng distVolMng;
    private final ApplicationContext ctx;

    @PersistenceContext
    private EntityManager em;

    @Autowired
    public ProcessMngImpl(@Lazy DistVolMng distVolMng, ConfigApp config, ThreadMng<Long> threadMng,
                          GenChrgProcessMng genChrgProcessMng, GenPenProcessMng genPenProcessMng,
                          MigrateMng migrateMng, ApplicationContext ctx) {
        this.distVolMng = distVolMng;
        this.config = config;
        this.threadMng = threadMng;
        this.genChrgProcessMng = genChrgProcessMng;
        this.genPenProcessMng = genPenProcessMng;
        this.migrateMng = migrateMng;
        this.ctx = ctx;
    }

    /**
     * Обработка запроса  из WebController
     *
     * @param tp       - тип выполнения 0-начисление, 1-задолженность и пеня, 2 - распределение объемов по вводу,
     *                 4 - начисление по одной услуге, для автоначисления
     * @param debugLvl - уровень отладки
     * @param genDt    - дата формирования
     * @param house    - дом
     * @param vvod     - ввод
     * @param ko       - объект Ко
     * @param uk       - УК
     * @param usl      - услуга
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class)
    public String processWebRequest(int tp, int debugLvl, Date genDt,
                                    House house, Vvod vvod, Ko ko, Org uk, Usl usl) {
        String retStatus;
        // построить запрос
        RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                .withTp(tp)
                .withGenDt(genDt)
                .withUk(uk)
                .withHouse(house)
                .withVvod(vvod)
                .withKo(ko)
                .withUsl(usl)
                .withCurDt1(config.getCurDt1())
                .withCurDt2(config.getCurDt2())
                .withDebugLvl(debugLvl)
                .withRqn(config.incNextReqNum())
                .withIsMultiThreads(true)
                .withStopMark("processMng.process")
                .build();
        reqConf.prepareId();
        //StopWatch sw = new StopWatch();
        //sw.start("TIMING: " + reqConf.getTpName());

        // проверить переданные параметры
        retStatus = reqConf.checkArguments();
        if (retStatus == null) {
            try {
                if (Utl.in(reqConf.getTp(), 0, 1, 2, 4)) {
                    // расчет начисления, распределение объемов, расчет задолженности и пени
                    reqConf.prepareChrgCountAmount();
                    log.trace("Будет обработано {} объектов", reqConf.getLstItems().size());
                    ProcessMng processMng = ctx.getBean(ProcessMng.class);
                    processMng.processAll(reqConf);
                }
                if (Utl.in(reqConf.getTp(), 4)) {
                    // по операции - начисление по одной услуге, для автоначисления
                    // вернуть начисленный объем
                    retStatus = "OK:" + reqConf.getChrgCountAmount().getResultVol().toString();
                } else {
                    retStatus = "OK";
                }
            } catch (Exception e) {
                retStatus = "ERROR! Ошибка при выполнении расчета!";
                log.error(Utl.getStackTraceString(e));
            }
        }
        //sw.stop();
        //System.out.println(sw.prettyPrint());
        return retStatus;
    }


    /**
     * Выполнение процесса формирования начисления, задолженности, по помещению, по дому, по вводу
     *
     * @param reqConf - конфиг запроса
     * @throws ErrorWhileGen - ошибка обработки
     */
    @Override
    @CacheEvict(value = {"ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    @Transactional(propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
    public void processAll(RequestConfigDirect reqConf) throws ErrorWhileGen {

        long startTime = System.currentTimeMillis();
        log.trace("НАЧАЛО процесса {} заданных объектов", reqConf.getTpName());
        // заблокировать, если нужно для долго длящегося процесса
        if (reqConf.isLockForLongLastingProcess()) {
            config.getLock().setLockProc(reqConf.getRqn(), reqConf.getStopMark());
        }
        try {
            // проверка остановки процесса
            boolean isCheckStop = false; // note решить что с этим делать!

            // ВЫЗОВ
            if (reqConf.isMultiThreads()) {
                // вызвать в новой транзакции, многопоточно
                threadMng.invokeThreads(reqConf, reqConf.getRqn());
            } else {
                // вызвать в той же транзакции, однопоточно, для Unit - тестов
                selectInvokeProcess(reqConf);
            }

        } finally {
            // разблокировать долго длящийся процесс
            if (reqConf.isLockForLongLastingProcess()) {
                config.getLock().unlockProc(reqConf.getRqn(), reqConf.getStopMark());
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime;
        String tpTime;
        if (reqConf.isSingleObjectCalc()) {
            // один объект - время в мс.
            totalTime = (endTime - startTime);
            tpTime = "мс.";
        } else {
            // много объектов - время в мин.
            totalTime = (endTime - startTime) / 60000L;
            tpTime = "мин.";
        }
        log.trace("");
        log.trace("ОКОНЧАНИЕ процесса {} заданных объектов - Общее время выполнения = {} {}",
                reqConf.getTpName(), totalTime, tpTime);
        log.trace("");
    }


    /**
     * Отдельный поток для расчета длительных процессов
     *
     * @param reqConf - конфиг запроса
     */
    @Async
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public CompletableFuture<CommonResult> process(RequestConfigDirect reqConf) throws ErrorWhileGen {
        long startTime = System.currentTimeMillis();
        log.trace("НАЧАЛО потока {}", reqConf.getTpName());

        selectInvokeProcess(reqConf);

        CommonResult res = new CommonResult(-111111, 0);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.trace("ОКОНЧАНИЕ потока {}, время расчета={} мс", reqConf.getTpName(), totalTime);
        return CompletableFuture.completedFuture(res);
    }


    /**
     * Обработать очередь объектов
     *
     * @param reqConf - конфиг запроса
     */
    private void selectInvokeProcess(RequestConfigDirect reqConf) throws ErrorWhileGen {
        switch (reqConf.getTp()) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4: {
                // перебрать все объекты для расчета
                Long id = null;
                try {
                    long i = 0, i2 = 0;
                    while (true) {
                        id = reqConf.getNextItem();
                        if (id != null) {
                            if (reqConf.isLockForLongLastingProcess() && config.getLock().isStopped(reqConf.getStopMark())) {
                                log.info("Процесс {} был ПРИНУДИТЕЛЬНО остановлен", reqConf.getTpName());
                                break;
                            }
                            if (Utl.in(reqConf.getTp(), 0, 1, 3, 4)) {
                                // Начисление и начисление для распределения объемов, расчет пени
                                if (reqConf.isSingleObjectCalc()) {
                                    log.trace("****** {} фин.лиц.сч. klskId={} - начало    ******",
                                            reqConf.getTpName(), id);
                                }
                                if (Utl.in(reqConf.getTp(), 1)) {
                                    // расчет пени
                                    // fixme Используется ли данный метод? 22.09.20
                                    genPenProcessMng.genDebitPen(reqConf.getCalcStore(), true, id);
                                } else {
                                    // расчет начисления и начисления для распределения объемов
                                    genChrgProcessMng.genChrg(reqConf, id);
                                }
                                if (reqConf.isSingleObjectCalc()) {
                                    log.trace("****** {} фин.лиц.сч. klskId={} - окончание   ******",
                                            reqConf.getTpName(), id);
                                } else if (i == 500L) {
                                    i = 0;
                                    log.info("****** Поток {}, {}, обработано {} объектов  ******",
                                            Thread.currentThread().getName(), reqConf.getTpName(), i2);
                                }
                                i++;
                                i2++;
                            } else if (reqConf.getTp() == 2) {
                                // Распределение объемов
                                distVolMng.distVolByVvodTrans(reqConf, id);
                            }
                        } else {
                            // перебраны все id, выход
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error(Utl.getStackTraceString(e));
                    // остановить другие потоки
                    if (reqConf.isLockForLongLastingProcess()) {
                        config.getLock().unlockProc(reqConf.getRqn(), reqConf.getStopMark());
                    }
                    if (reqConf.getTp() == 2) {
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект vvodId=" + id);
                    } else {
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект klskId=" + id);
                    }
                }
                break;
            }
            case 5: {
                // миграция долгов
                // перебрать все объекты для расчета
                String id = null;
                try {
                    long i = 0;
                    while (true) {
                        id = reqConf.getNextStrItem();
                        if (id != null) {
                            if (reqConf.isLockForLongLastingProcess() && config.getLock().isStopped(reqConf.getStopMark())) {
                                log.info("Процесс {} был ПРИНУДИТЕЛЬНО остановлен", reqConf.getTpName());
                                break;
                            }
                            migrateMng.migrateDeb(id, Integer.valueOf(config.getPeriodBack()),
                                    Integer.valueOf(config.getPeriod()), reqConf.getDebugLvl());
                            log.info("****** Поток {}, {}, обработано {} объектов ******",
                                    Thread.currentThread().getName(), reqConf.getTpName(), i);
                            i++;
                        } else {
                            // перебраны все id, выход
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error(Utl.getStackTraceString(e));
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект lsk=" + id);
                }
                break;
            }
            default:
                throw new ErrorWhileGen("Некорректный параметр reqConf.tp=" + reqConf.getTp());
        }
    }


}