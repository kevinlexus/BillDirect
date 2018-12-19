package com.dic.app.mm.impl;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.app.mm.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.excp.WrongParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ProcessMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
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
    private ThreadMng<String> threadMng;
    @Autowired
    private GenPenProcessMng genPenProcessMng;
    @Autowired
    private GenChrgProcessMng genChrgProcessMng;

    @Autowired
    private ApplicationContext ctx;

    @PersistenceContext
    private EntityManager em;

    /**
     * Выполнение процесса формирования
     *
     * @param lskFrom  - начальный лиц.счет, если отсутствует - весь фонд
     * @param lskTo    - конечный лиц.счет, если отсутствует - весь фонд
     * @param genDt    - дата расчета
     * @param debugLvl - уровень отладочной информации (0-нет, 1-отобразить)
     * @param reqConf  - конфиг запроса
     * @throws ErrorWhileChrgPen
     */
    @Override
    @CacheEvict(value = {"ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    @Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void genProcessAll(String lskFrom, String lskTo, Date genDt,
                              Integer debugLvl, RequestConfig reqConf) throws ErrorWhileChrgPen {
        // маркер, для проверки необходимости остановки
        String mark = null;
        if (!lskFrom.equals(lskTo)) {
            // по нескольким лиц.счетам
            mark = "processMng.genProcessAll";
            // установить маркер процесса
            config.getLock().setLockProc(reqConf.getRqn(), mark);
        }

        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО расчета по типу={}", reqConf.getTp());

        // загрузить справочники
        CalcStore calcStore = buildCalcStore(genDt, debugLvl);
        // получить список лицевых счетов
        List<String> lstItem;
        lstItem = kartDao.getRangeLsk(lskFrom, lskTo)
                .stream().map(t -> t.getLsk()).collect(Collectors.toList());

        // lambda, будет выполнено позже, в создании потока
        PrepThread<String> reverse = (item, proc) -> {
            ProcessMng processMng = ctx.getBean(ProcessMng.class);
            return processMng.genProcess(item, calcStore, reqConf);
        };

        // вызвать в потоках
        try {
            threadMng.invokeThreads(reverse, CNT_THREADS, lstItem, mark);
        } catch (InterruptedException | ExecutionException | WrongParam e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileChrgPen("ОШИБКА во время расчета!");
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ процесса по типу={} - Общее время выполнения={}",reqConf.getTp(), totalTime);
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

        log.info("Загружен справочник ставок рефинансирования");
        return calcStore;
    }

    /**
     * Процессинг расчета по одному лиц.счету
     *
     * @param lsk       - лиц.счет
     * @param calcStore - хранилище справочников
     * @param reqConf   - конфиг запроса
     */
    @Async
    @Override
    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public Future<CommonResult> genProcess(String lsk, CalcStore calcStore, RequestConfig reqConf) throws WrongParam {
        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО процесса по типу={}, по лиц.счету {}", reqConf.getTp(), lsk);
        try {
            // заблокировать лиц.счет для расчета
            if (!config.aquireLock(reqConf.getRqn(), lsk)) {
                throw new RuntimeException("ОШИБКА БЛОКИРОВКИ лc.=" + lsk);
            }
            Kart kart = em.find(Kart.class, lsk);

            switch (reqConf.getTp()) {
                case 0: {
                    // начисление
                    genChrgProcessMng.genChrg(calcStore, kart);
                }
                case 1: {
                    // расчет ДОЛГА и ПЕНИ
                    genPenProcessMng.genDebitPen(calcStore, kart);
                    break;
                }
                default:
                    throw new WrongParam("Некорректный параметр reqConf.tp=" + reqConf.getTp());
            }
        } finally {
            // разблокировать лицевой счет
            config.getLock().unlockLsk(reqConf.getRqn(), lsk);
        }
        CommonResult res = new CommonResult(lsk, 0);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ расчета по типу={}, по лиц.счету {} Время расчета={} мс", reqConf.getTp(), lsk, totalTime);
        return new AsyncResult<CommonResult>(res);
    }

}