package com.dic.app.mm.impl;

import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.ChrgCount;
import com.dic.bill.dto.ChrgVol;
import com.dic.bill.dto.CountPers;
import com.dic.bill.mm.KartPrMng;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.mm.NaborMng;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.*;

/**
 * Сервис расчета начисления
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
@Scope("prototype")
public class GenChrgProcessMngImpl implements GenChrgProcessMng {

    @Autowired
    private NaborMng naborMng;
    @Autowired
    private StatesPrDAO statesPrDao;
    @Autowired
    private KartPrMng kartPrMng;
    @Autowired
    private MeterMng meterMng;
    @Autowired
    private SprParamMng sprParamMng;
    @PersistenceContext
    private EntityManager em;

    /**
     * Рассчитать начисление
     *
     * @param calcStore - хранилище справочников
     * @param kart      - лиц.счет
     */
    @Override
    public void genChrg(CalcStore calcStore, Kart kart) throws WrongParam {
        List<StatePr> lstStatesPr = statesPrDao.findByDate(kart.getLsk(),
                calcStore.getCurDt1(), calcStore.getCurDt2());
        // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
        Double parVarCntKpr =
                Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"),0D); // применил NVL, так как много кода
                                                                    // в Oracle использовало NVL от NULL параметра...

        ChrgCount chrgCount = new ChrgCount();
        // цикл по дням месяца
        Calendar c = Calendar.getInstance();
        for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();
            log.info("Date={}", curDt);
            List<Nabor> lst = naborMng.getValidNabor(kart, curDt);
            lst.forEach(t -> {
                log.info("Usl.id={}, name={}", t.getUsl().getId(), t.getUsl().getName());
                // получить кол-во проживающих
                CountPers countPers = kartPrMng.getCountPersByDate(kart, parVarCntKpr, lstStatesPr, t.getUsl(), curDt);
                // получить наличие счетчика
                Meter meter = null ;
                if (t.getUsl().getCounter() != null) {
                    meter = meterMng.getActualMeterByKoUsl(kart.getKoKw(), t.getUsl().getId(), curDt);
                }
                if (meter != null) {
                    log.info("Meter: id={}, dt1={}, dt2={}, usl={}",
                            meter.getId(), meter.getDt1(), meter.getDt2(), meter.getUsl().getId());
                }
                // сгруппировать
                addChrgVol(chrgCount, t.getUsl(), meter != null,
                        calcStore.getDayPartMonth(), countPers);
            });
        }
        // получить кол-во проживающих по лиц.счету
        log.info("Расчет:");
        for (Map.Entry<Usl, Integer> e : chrgCount.getMapKprMax().entrySet()) {
            log.info("usl={} kprMax={}", e.getKey().getId(), e.getValue());
        }


        for (Map.Entry<Usl, List<ChrgVol>> e : chrgCount.getMapChrgVol().entrySet()) {
            log.info("usl={} ", e.getKey().getId());
            for (ChrgVol t : e.getValue()) {
                log.info("ChrgVol: t.isCounter={}, t.isEmpty={}, t.kpr={}, t.kprNorm={}, t.kprOt={}, t.kprWr={}",
                        t.isCounter, t.isEmpty, t.kpr.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.kprNorm.setScale(5, BigDecimal.ROUND_HALF_UP), t.kprOt.setScale(5,
                        BigDecimal.ROUND_HALF_UP), t.kprWr.setScale(5, BigDecimal.ROUND_HALF_UP));
            }

        }

    }

    /**
     * Добавить и сгруппировать объем по услуге
     * @param chrgCount - объемы и кол-во прожив. по услуге
     * @param usl - услуга
     * @param isCounter - наличие счетчика
     * @param dayPartMonth - доля дня в месяце
     * @param countPers - кол-во проживающих
     */
    private void addChrgVol(ChrgCount chrgCount, Usl usl,
                            boolean isCounter,
                            BigDecimal dayPartMonth, CountPers countPers) {
        List<ChrgVol> lstChrgVol = chrgCount.getMapChrgVol().get(usl);
        if (lstChrgVol == null) {
            lstChrgVol = new ArrayList<>();
            chrgCount.getMapChrgVol().put(usl, lstChrgVol);
        }
        ChrgVol foundChrgVol = null;
        if (lstChrgVol.size() !=0) {
            // получить записи с такими же ключевыми параметрами - наличия счетчика и пустой квартиры
            lstChrgVol.forEach(t-> log.info("TEST t.isCounter={}, t.isEmpty={}", t.isCounter, t.isEmpty));
            foundChrgVol = lstChrgVol.stream()
                    .filter(t -> t.isCounter == isCounter && t.isEmpty == countPers.isEmpty)
                    .findFirst().orElse(null);
        }
        if (lstChrgVol.size()==0 || foundChrgVol == null) {
            // создать запись
            ChrgVol chrgVol = new ChrgVol(usl, countPers.isEmpty, isCounter);
            chrgVol.kpr = dayPartMonth.multiply(BigDecimal.valueOf(countPers.kpr));
            chrgVol.kprNorm = dayPartMonth.multiply(BigDecimal.valueOf(countPers.kprNorm));
            chrgVol.kprWr = dayPartMonth.multiply(BigDecimal.valueOf(countPers.kprWr));
            chrgVol.kprOt = dayPartMonth.multiply(BigDecimal.valueOf(countPers.kprOt));
            chrgVol.isEmpty = countPers.isEmpty;
            // добавить долю дня
            chrgVol.partMonth = chrgVol.partMonth.add(dayPartMonth);
            lstChrgVol.add(chrgVol);
        } else {
            // добавить в существующую запись объема
            foundChrgVol.kpr = foundChrgVol.kpr.add(dayPartMonth.multiply(BigDecimal.valueOf(countPers.kpr)));
            foundChrgVol.kprNorm = foundChrgVol.kprNorm.add(dayPartMonth.multiply(BigDecimal.valueOf(countPers.kprNorm)));
            foundChrgVol.kprWr = foundChrgVol.kprWr.add(dayPartMonth.multiply(BigDecimal.valueOf(countPers.kprWr)));
            foundChrgVol.kprOt = foundChrgVol.kprOt.add(dayPartMonth.multiply(BigDecimal.valueOf(countPers.kprOt)));
            // добавить долю дня
            foundChrgVol.partMonth = foundChrgVol.partMonth.add(dayPartMonth);
        }

        // сохранить максимальное кол-во проживающих, по услуге
        Integer kprMax = chrgCount.getMapKprMax().get(usl);
        if (kprMax == null ||countPers.kprMax > kprMax) {
            chrgCount.getMapKprMax().put(usl, countPers.kprMax);
        }
    }

}