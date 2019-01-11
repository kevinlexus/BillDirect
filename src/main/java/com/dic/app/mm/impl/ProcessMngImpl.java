package com.dic.app.mm.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.app.mm.*;
import com.dic.bill.dto.ChrgCount;
import com.dic.bill.dto.ChrgCountHouse;
import com.dic.bill.model.scott.*;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ProcessMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dto.CalcStore;
import com.ric.dto.CommonResult;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис выполнения процессов формирования
 *
 * @author lev
 * @version 1.12
 */
@Slf4j
@Service
@Scope("prototype")
public class ProcessMngImpl implements ProcessMng {

    final int CNT_THREADS = 15;
    @Autowired
    private ConfigApp config;
    @Autowired
    private PenDtDAO penDtDao;
    @Autowired
    private PenRefDAO penRefDao;
    @Autowired
    private KartDAO kartDao;
    @Autowired
    private ThreadMng<Integer> threadMng;
    @Autowired
    private GenPenProcessMng genPenProcessMng;
    @Autowired
    private GenChrgProcessMng genChrgProcessMng;

    @Autowired
    private ApplicationContext ctx;

    @PersistenceContext
    private EntityManager em;

    /**
     * Выполнение процесса формирования либо по квартире, по дому, по вводу
     *
     * @param reqConf  - конфиг запроса
     * @param calcStore - хранилище справочников
     * @throws ErrorWhileChrgPen
     */
    @Override
    @CacheEvict(value = {"ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void genProcessAll(RequestConfig reqConf, CalcStore calcStore) throws ErrorWhileChrgPen {

        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО обработки всех объектов, по типу={}", reqConf.getTp());

        // получить список квартир
        List<Integer> lstItem = null;
        String stopMark = null;
        List<Integer> lstMark = new ArrayList<>();
        // тип выборки
        int tpSel;
        Ko ko = reqConf.getKo();
        House house = reqConf.getHouse();
        Vvod vvod = reqConf.getVvod();
        if (ko != null) {
            // по квартире
            tpSel = 1;
            lstItem = new ArrayList<>(1);
            lstItem.add(ko.getId());
            lstMark.add(ko.getId());
            lstMark.add(house.getKo().getId());
        } else if (house != null) {
            // по дому
            tpSel = 2;
            lstItem = kartDao.getKoByHouse(house).stream().map(t->t.getId()).collect(Collectors.toList());
            lstMark.add(house.getKo().getId());
        } else if (vvod != null) {
            // по вводу
            tpSel = 3;
            lstItem = kartDao.getKoByVvod(vvod).stream().map(t->t.getId()).collect(Collectors.toList());
        } else {
            tpSel = 0;
            // по всему фонду
            lstItem = kartDao.getKoAll().stream().map(t->t.getId()).collect(Collectors.toList());
            // маркер, для проверки необходимости остановки (только если весь фонд задан для расчета)
            stopMark = "processMng.genProcess";
            // установить маркер процесса
            config.getLock().setLockProc(reqConf.getRqn(), stopMark);
        }


        // lambda, будет выполнено позже, в создании потока
        PrepThread<Integer> reverse = (item, proc) -> {
            log.info("************** 1");
            ProcessMng processMng = ctx.getBean(ProcessMng.class);
            log.info("************** 2");
            return processMng.genProcess(item, calcStore, reqConf);
        };

        // вызвать в потоках
        try {
            threadMng.invokeThreads(reverse, CNT_THREADS, lstItem, stopMark);
        } catch (InterruptedException | ExecutionException | WrongParam | ErrorWhileChrg e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileChrgPen("ОШИБКА во время расчета!");
        } finally {
            if (tpSel==0) {
                // снять маркер процесса
                config.getLock().unlockProc(reqConf.getRqn(), stopMark);
            }
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ обработки всех объектов, по типу={} - Общее время выполнения={}",reqConf.getTp(), totalTime);
    }

    /**
     * Загрузить справочники в хранилище
     * @param genDt - дата расчета
     * @param debugLvl - уровень отладочной информации (0-нет, 1-отобразить)
     * @return
     */
    @Override
    public CalcStore buildCalcStore(Date genDt, Integer debugLvl) {
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
        // справочник дат начала пени
        calcStore.setLstPenDt(penDtDao.findAll());
        log.info("Загружен справочник дат начала обязательства по оплате");
        // справочник ставок рефинансирования
        calcStore.setLstPenRef(penRefDao.findAll());
        // хранилище параметров по дому (для ОДН и прочих нужд)
        calcStore.setChrgCountHouse(new ChrgCountHouse());

        log.info("Загружен справочник ставок рефинансирования");
        return calcStore;
    }

    /**
     * Процессинг расчета по одной квартире
     *
     * @param klskId - Id квартиры
     * @param calcStore - хранилище справочников
     * @param reqConf   - конфиг запроса
     */
    @Async
    @Override
    @Transactional(readOnly = false,
            propagation = Propagation.REQUIRES_NEW, // новая транзакция, Не ставить Propagation.MANADATORY! - не даёт запустить поток!
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class) //
    public Future<CommonResult> genProcess(int klskId, CalcStore calcStore, RequestConfig reqConf) throws WrongParam, ErrorWhileChrg {
        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО потока по типу={}, по klskId={}", reqConf.getTp(), klskId);
        try {
            // заблокировать объект Ko для расчета
            if (!config.aquireLock(reqConf.getRqn(), klskId)) {
                throw new RuntimeException("ОШИБКА БЛОКИРОВКИ klskId=" + klskId);
            }
            Ko ko = em.find(Ko.class, klskId);
            log.info("******* klskId={}", klskId);

            switch (reqConf.getTp()) {
                case 0: {
                    // начисление
                    genChrgProcessMng.genChrg(calcStore, ko);
                }
                case 1: {
                    // расчет ДОЛГА и ПЕНИ -  TODO переделать на Ko!!! ред. 11.01.19
                    //genPenProcessMng.genDebitPen(calcStore, kart);
                    break;
                }
                default:
                    throw new WrongParam("Некорректный параметр reqConf.tp=" + reqConf.getTp());
            }
        } finally {
            // разблокировать лицевой счет
            config.getLock().unlockLsk(reqConf.getRqn(), klskId);
        }
        CommonResult res = new CommonResult(klskId, 0);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ потока по типу={}, по klskId {} Время расчета={} мс", reqConf.getTp(), klskId, totalTime);
        return new AsyncResult<CommonResult>(res);
    }

}