package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.*;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dao.VvodDAO;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceContextType;
import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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

    @Autowired
    private ConfigApp config;
    @Autowired
    private PenDtDAO penDtDao;
    @Autowired
    private PenRefDAO penRefDao;
    @Autowired
    private KartDAO kartDao;
    @Autowired
    private KartMng kartMng;
    @Autowired
    private ThreadMng<Long> threadMng;
    @Autowired
    private GenPenProcessMng genPenProcessMng;
    @Autowired
    private GenChrgProcessMng genChrgProcessMng;

    private final DistVolMng distVolMng;

    @Autowired
    private VvodDAO vvodDAO;

    @Autowired
    private ApplicationContext ctx;
    @PersistenceContext
    private EntityManager em;

    @Autowired
    public ProcessMngImpl(@Lazy DistVolMng distVolMng) {
        this.distVolMng = distVolMng;
    }

    /**
     * Распределить объемы по вводу (по всем вводам, если reqConf.vvod == null)
     *
     * @param reqConf - параметры запроса
     */
/*
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void distVolAll(RequestConfigDirect reqConf)
            throws ErrorWhileGen {
        if (reqConf.getTp() != 2) {
            throw new ErrorWhileGen("ОШИБКА! Задан некорректный тип выполнения");
        }
        // заблокировать, если нужно для долго длящегося процесса
        if (reqConf.isLockForLongLastingProcess()) {
            config.getLock().setLockProc(reqConf.getRqn(), stopMark);
        }
        try {
            if (reqConf.getVvod() != null) {
                // распределить конкретный ввод
                try {
                    if (reqConf.isMultiThreads()) {
                        // вызвать в новой транзакции, многопоточно
                        distVolMng.distVolByVvodTrans(reqConf, reqConf.getVvod().getId());
                    } else {
                        // вызвать в той же транзакции, однопоточно, для Unit - тестов
                        distVolMng.distVolByVvodSameTrans(reqConf, reqConf.getVvod().getId());
                    }
                    log.info("Распределение объемов по вводу vvodId={} выполнено", reqConf.getVvod().getId());
                } catch (ErrorWhileChrgPen | WrongParam | WrongGetMethod | ErrorWhileDist e) {
                    log.error(Utl.getStackTraceString(e));
                    throw new ErrorWhileGen("ОШИБКА при распределении объемов");
                }
            } else if (reqConf.getUk() != null) {
                for (Long vvodId : vvodDAO.findVvodByUk(reqConf.getUk().getReu())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList())) {
                    if (!config.getLock().isStopped(stopMark)) {
                        Vvod vvod = em.find(Vvod.class, vvodId);
                        distVolCommon(reqConf, vvod);
                    } else {
                        log.info("Процесс {} был ПРИНУДИТЕЛЬНО остановлен", reqConf.getTpName());
                        break;
                    }
                }
                log.info("Распределение объемов по УК reuId={} выполнено", reqConf.getUk().getReu());
            } else if (reqConf.getHouse() != null) {
                for (Vvod vvod : vvodDAO.findVvodByHouse(reqConf.getHouse().getId())) {
                    distVolCommon(reqConf, vvod);
                }
                log.info("Распределение объемов по дому houseId={} выполнено", reqConf.getHouse().getId());
            } else {
                // распределить все вводы
                for (Vvod vvod : vvodDAO.findAll(new Sort(Sort.Direction.ASC, "id"))) {
                    if (!config.getLock().isStopped(stopMark)) {
                        distVolCommon(reqConf, vvod);
                    }
                }
                log.info("Распределение объемов по всем вводам выполнено");
            }
        } finally {
            // разблокировать долго длящийся процесс
            if (reqConf.isLockForLongLastingProcess()) {
                config.getLock().unlockProc(reqConf.getRqn(), stopMark);
            }
        }
    }
*/

    /**
     * Распределить объемы
     *
     * @param reqConf - конфиг запроса
     * @param vvod    - ввод
     */

    private void distVolCommon(RequestConfigDirect reqConf, Vvod vvod) throws ErrorWhileGen {
        if (vvod.getUsl() != null && vvod.getUsl().isMain()) {
            try {
                distVolMng.distVolByVvodTrans(reqConf, vvod.getId());
            } catch (ErrorWhileChrgPen | WrongParam | WrongGetMethod | ErrorWhileDist e) {
                log.error(Utl.getStackTraceString(e));
                throw new ErrorWhileGen("ОШИБКА при распределении объемов");
            }
        }
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
    public void genProcessAll(RequestConfigDirect reqConf) throws ErrorWhileGen {

        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО процесса {} заданных объектов", reqConf.getTpName());

        // проверка остановки процесса
        boolean isCheckStop = false; // note решить что с этим делать!

        // LAMBDA, будет выполнено позже, в создании потока
        PrepThread<Long> reverse = (item, proc) -> {
            //log.info("************** 1");
            ProcessMng processMng = ctx.getBean(ProcessMng.class);
            //log.info("************** 2");
            return processMng.genProcess(reqConf);
        };

        // ВЫЗОВ
        try {
            if (reqConf.isMultiThreads()) {
                // вызвать в новой транзакции, многопоточно
                // note Здесь потоки не назначаются, только вызов.
                // note Настраивать это значение совместно с Config.java.getAsyncExecutor()
                // note А так же application.properties spring.datasource.hikari.maximumPoolSize
                int CNT_THREADS = 15;
                threadMng.invokeThreads(reverse, CNT_THREADS, isCheckStop, reqConf.getRqn(), stopMark);
            } else {
                // вызвать в той же транзакции, однопоточно, для Unit - тестов
                selectInvokeProcess(reqConf);
            }
/*
        } catch (InterruptedException | ExecutionException | WrongParam | ErrorWhileChrg
                | WrongGetMethod | ErrorWhileChrgPen | ErrorWhileDist e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileGen("ОШИБКА во время расчета!");
*/
        } finally {
            //if (reqConf.tpSel == 0) {
            // снять маркер процесса
            //    config.getLock().unlockProc(reqConf.getRqn(), stopMark);
            //}
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ процесса {} заданных объектов - Общее время выполнения={}", reqConf.getTpName(), totalTime);

    }


    /**
     * Отдельный поток для расчета длительных процессов
     *
     * @param reqConf - конфиг запроса
     */
    @Async
    @Override
    @Transactional/*(propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)*/
    public Future<CommonResult> genProcess(RequestConfigDirect reqConf) throws ErrorWhileGen {
        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО потока {}", reqConf.getTpName());

        selectInvokeProcess(reqConf);

        CommonResult res = new CommonResult(-111111, 0);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ потока {}, время расчета={} мс", reqConf.getTpName(), totalTime);
        return new AsyncResult<>(res);
    }


    /**
     * Выбрать и вызвать поток
     *
     * @param reqConf - конфиг запроса
     */
    private void selectInvokeProcess(RequestConfigDirect reqConf) throws ErrorWhileGen {
        switch (reqConf.getTp()) {
            case 0:
            case 2:
            case 3: {
                // перебрать все помещения для расчета
                // заблокировать, если нужно для долго длящегося процесса
                if (reqConf.isLockForLongLastingProcess()) {
                    config.getLock().setLockProc(reqConf.getRqn(), stopMark);
                }
                try {
                    while (true) {
                        Long id = reqConf.getNextItem();
                        if (id != null) {
                            if (reqConf.isLockForLongLastingProcess() && config.getLock().isStopped(stopMark)) {
                                log.info("Процесс {} был ПРИНУДИТЕЛЬНО остановлен", reqConf.getTpName());
                                break;
                            }
                            if (Utl.in(reqConf.getTp(),0,3)) {
                                // Начисление и начисление для распределения объемов
                                genChrgProcessMng.genChrg(reqConf, id);
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
                    throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке "+reqConf.getTpName());
                } finally {
                    // разблокировать долго длящийся процесс
                    if (reqConf.isLockForLongLastingProcess()) {
                        config.getLock().unlockProc(reqConf.getRqn(), stopMark);
                    }
                }
                break;
            }
            case 1: {
                // Расчет долга и пени
                //genPenProcessMng.genDebitPen(calcStore, klskId); fixme доделать вызов!
                break;
            }
            default:
                throw new ErrorWhileGen("Некорректный параметр reqConf.tp=" + reqConf.getTp());
        }
    }


}