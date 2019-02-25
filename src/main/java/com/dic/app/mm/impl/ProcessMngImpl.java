package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dao.VvodDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.ChrgCountAmount;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.collection.PredicatedCollection;
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collector;
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
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void distVol(RequestConfig reqConf)
            throws ErrorWhileGen {
        if (reqConf.getTp() != 2) {
            throw new ErrorWhileGen("ОШИБКА! Задан некорректный тип выполнения");
        }
        // установить маркер процесса, вернуться, если уже выполняется
        if (config.getLock().setLockProc(reqConf.getRqn(), stopMark)) {
            try {
                if (reqConf.getVvod() != null) {
                    // загрузить хранилище
                    CalcStore calcStore = buildCalcStore(reqConf);
                    // распределить конкретный ввод
                    try {
                        if (reqConf.isMultiThreads()) {
                            // вызвать в новой транзакции, многопоточно
                            distVolMng.distVolByVvodTrans(reqConf, calcStore, reqConf.getVvod().getId());
                        } else {
                            // вызвать в той же транзакции, однопоточно, для Unit - тестов
                            distVolMng.distVolByVvodSameTrans(reqConf, calcStore, reqConf.getVvod().getId());
                        }
                        log.info("Распределение объемов по вводу vvodId={} выполнено", reqConf.getVvod().getId());
                    } catch (ErrorWhileChrgPen | WrongParam | WrongGetMethod | ErrorWhileDist e) {
                        log.error(Utl.getStackTraceString(e));
                        throw new ErrorWhileGen("ОШИБКА при распределении объемов");
                    }
                } else if (reqConf.getUk() != null) {
                    for (Integer vvodId : vvodDAO.findVvodByUk(reqConf.getUk().getReu())
                            .stream().map(BigDecimal::intValue).collect(Collectors.toList())) {
                        Vvod vvod = em.find(Vvod.class, vvodId);
                        if (vvod.getUsl() != null && vvod.getUsl().isMain()) {
                            // загрузить хранилище по каждому вводу
                            CalcStore calcStore = buildCalcStore(reqConf);
                            try {
                                distVolMng.distVolByVvodTrans(reqConf, calcStore, vvod.getId());
                            } catch (ErrorWhileChrgPen | WrongParam | WrongGetMethod | ErrorWhileDist e) {
                                log.error(Utl.getStackTraceString(e));
                                throw new ErrorWhileGen("ОШИБКА при распределении объемов");
                            }
                        }
                    }
                    log.info("Распределение объемов по УК reuId={} выполнено", reqConf.getUk().getReu());
                } else if (reqConf.getHouse() != null) {
                    for (Vvod vvod : vvodDAO.findVvodByHouse(reqConf.getHouse().getId())) {
                        if (vvod.getUsl() != null && vvod.getUsl().isMain()) {
                            // загрузить хранилище по каждому вводу
                            CalcStore calcStore = buildCalcStore(reqConf);
                            try {
                                distVolMng.distVolByVvodTrans(reqConf, calcStore, vvod.getId());
                            } catch (ErrorWhileChrgPen | WrongParam | WrongGetMethod | ErrorWhileDist e) {
                                log.error(Utl.getStackTraceString(e));
                                throw new ErrorWhileGen("ОШИБКА при распределении объемов");
                            }
                        }
                    }
                    log.info("Распределение объемов по дому houseId={} выполнено", reqConf.getHouse().getId());
                } else {
                    // распределить все вводы
                    for (Vvod vvod : vvodDAO.findAll(new Sort(Sort.Direction.ASC, "id"))) {
                        //log.info("Удалить это сообщение vvodId={}", vvod.getId());
                        if (vvod.getUsl() != null && vvod.getUsl().isMain()) {
                            if (!config.getLock().isStopped(stopMark)) {
                                // загрузить хранилище по каждому вводу
                                CalcStore calcStore = buildCalcStore(reqConf);
                                try {
                                    distVolMng.distVolByVvodTrans(reqConf, calcStore, vvod.getId());
                                } catch (ErrorWhileChrgPen | WrongParam | WrongGetMethod | ErrorWhileDist e) {
                                    log.error(Utl.getStackTraceString(e));
                                    throw new ErrorWhileGen("ОШИБКА при распределении объемов");
                                }
                            }
                        }
                    }
                    log.info("Распределение объемов по всем вводам выполнено");
                }
            } finally {
                // снять маркер процесса
                config.getLock().unlockProc(reqConf.getRqn(), stopMark);
            }
        }
    }

    /**
     * Выполнение процесса формирования начисления, задолженности, по помещению, по дому, по вводу
     *
     * @param reqConf   - конфиг запроса
     * @param calcStore - хранилище справочников
     * @throws ErrorWhileGen - ошибка обработки
     */
    @Override
    @CacheEvict(value = {"ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void genProcessAll(RequestConfig reqConf, CalcStore calcStore) throws ErrorWhileGen {

        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО процесса {} заданных объектов", reqConf.getTpName());

        // получить список помещений
        List<Long> lstItems;
        // тип выборки
        int tpSel;
        Ko ko = reqConf.getKo();
        House house = reqConf.getHouse();
        Org uk = reqConf.getUk();
        Vvod vvod = reqConf.getVvod();
        // проверка остановки процесса
        boolean isCheckStop = false;
        if (ko != null) {
            // по помещению
            tpSel = 1;
            lstItems = new ArrayList<>(1);
            lstItems.add(ko.getId());
        } else if (uk != null) {
            // по УК
            tpSel = 0;
            lstItems = kartDao.findAllByUk(uk.getReu()).stream().map(BigDecimal::longValue).collect(Collectors.toList());
            // установить маркер процесса, вернуться, если уже выполняется
            if (!config.getLock().setLockProc(reqConf.getRqn(), stopMark)) {
                return;
            } else {
                isCheckStop = true;
            }
        } else if (house != null) {
            // по дому
            tpSel = 2;
            lstItems = kartMng.getKoByHouse(house).stream().map(Ko::getId).collect(Collectors.toList());
        } else if (vvod != null) {
            // по вводу
            tpSel = 3;
            lstItems = kartMng.getKoByVvod(vvod).stream().map(Ko::getId).collect(Collectors.toList());
        } else {
            tpSel = 0;
            // по всему фонду
            // конвертировать из List<BD> в List<Long> (native JPA представляет k_lsk_id только в BD и происходит type Erasure)
            lstItems = kartDao.findAllKlskId().stream().map(BigDecimal::longValue).collect(Collectors.toList());
            // установить маркер процесса, вернуться, если уже выполняется
            if (!config.getLock().setLockProc(reqConf.getRqn(), stopMark)) {
                return;
            } else {
                isCheckStop = true;
            }
        }

        // LAMBDA, будет выполнено позже, в создании потока
        PrepThread<Long> reverse = (item, proc) -> {
            //log.info("************** 1");
            ProcessMng processMng = ctx.getBean(ProcessMng.class);
            //log.info("************** 2");
            return processMng.genProcess(item, calcStore, reqConf);
        };

        // ВЫЗОВ
        try {
            if (reqConf.isMultiThreads()) {
                // вызвать в новой транзакции, многопоточно
                int CNT_THREADS = 15;
                threadMng.invokeThreads(reverse, CNT_THREADS, lstItems, isCheckStop, reqConf.getRqn(), stopMark);
            } else {
                // вызвать в той же транзакции, однопоточно, для Unit - тестов
                for (Long klskId : lstItems) {
                    selectInvokeProcess(reqConf, calcStore, klskId);
                }
            }
        } catch (InterruptedException | ExecutionException | WrongParam | ErrorWhileChrg e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileGen("ОШИБКА во время расчета!");
        } finally {
            if (tpSel == 0) {
                // снять маркер процесса
                config.getLock().unlockProc(reqConf.getRqn(), stopMark);
            }
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ процесса {} заданных объектов - Общее время выполнения={}", reqConf.getTpName(), totalTime);

    }

    /**
     * Загрузить справочники в хранилище
     *
     * @param reqConf
     * @return - хранилище
     */
    @Override
    public CalcStore buildCalcStore(RequestConfig reqConf) {
        int debugLvl = reqConf.getDebugLvl();
        int tp = reqConf.getTp();
        Date genDt = reqConf.getGenDt();
        CalcStore calcStore = new CalcStore();
        // уровень отладки
        calcStore.setDebugLvl(debugLvl);
        // дата начала периода
        calcStore.setCurDt1(config.getCurDt1());
        // дата окончания периода
        calcStore.setCurDt2(config.getCurDt2());
        // дата расчета пени
        calcStore.setGenDt(genDt);
        // текущий период
        calcStore.setPeriod(Integer.valueOf(config.getPeriod()));
        // период - месяц назад
        calcStore.setPeriodBack(Integer.valueOf(config.getPeriodBack()));
        log.info("Начало получения справочников");
        if (tp == 1) {
            // справочник дат начала пени
            calcStore.setLstPenDt(penDtDao.findAll());
            log.info("Загружен справочник дат начала обязательства по оплате");
            // справочник ставок рефинансирования
            calcStore.setLstPenRef(penRefDao.findAll());
            log.info("Загружен справочник ставок рефинансирования");
        }
        // хранилище параметров по дому (для ОДН и прочих нужд)
        calcStore.setChrgCountAmount(new ChrgCountAmount());

        return calcStore;
    }

    /**
     * Процессинг расчета по одному помещению
     *
     * @param klskId    - Id помещения
     * @param calcStore - хранилище справочников
     * @param reqConf   - конфиг запроса
     */
    @Async
    @Override
    @Transactional(
            propagation = Propagation.REQUIRES_NEW, // новая транзакция, Не ставить Propagation.MANADATORY! - не даёт запустить поток!
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class) //
    public Future<CommonResult> genProcess(long klskId, CalcStore calcStore, RequestConfig reqConf) throws WrongParam, ErrorWhileChrg {
        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО потока {}, по klskId={}", reqConf.getTpName(), klskId);
        try {
            // заблокировать объект Ko для расчета
            if (!config.aquireLock(reqConf.getRqn(), klskId)) {
                throw new RuntimeException("ОШИБКА БЛОКИРОВКИ klskId=" + klskId);
            }
            log.info("******* klskId={} заблокирован для расчета", klskId);

            selectInvokeProcess(reqConf, calcStore, klskId);
        } catch (WrongParam | ErrorWhileChrg e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileChrg("ОШИБКА во время расчета!");
        } finally {
            // разблокировать лицевой счет
            config.getLock().unlockLsk(reqConf.getRqn(), klskId);
            log.info("******* klskId={} разблокирован после расчета", klskId);
        }
        CommonResult res = new CommonResult(klskId, 0);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ потока {}, по klskId {} Время расчета={} мс", reqConf.getTpName(), klskId, totalTime);
        return new AsyncResult<>(res);
    }

    /**
     * Выбрать и вызвать процесс
     *
     * @param reqConf   - конфиг запроса
     * @param calcStore - хранилище параметров
     * @param klskId    - klskId объекта
     */
    private void selectInvokeProcess(RequestConfig reqConf, CalcStore calcStore, long klskId) throws WrongParam, ErrorWhileChrg {
        switch (reqConf.getTp()) {
            case 0:
            case 2: {
                // начисление и расчет объемов
                genChrgProcessMng.genChrg(calcStore, klskId, reqConf);
                break;
            }
            case 1: {
                // расчет долга и пени
                genPenProcessMng.genDebitPen(calcStore, klskId);
                break;
            }
            default:
                throw new WrongParam("Некорректный параметр reqConf.tp=" + reqConf.getTp());
        }
    }

}