package com.dic.app.mm.impl;

import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dto.*;
import com.dic.bill.mm.*;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Nabor;
import com.dic.bill.model.scott.StatePr;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
    private KartMng kartMng;
    @Autowired
    private KartPrMng kartPrMng;
    @Autowired
    private MeterMng meterMng;
    @Autowired
    private MeterDAO meterDao;
    @Autowired
    private SprParamMng sprParamMng;
    @PersistenceContext
    private EntityManager em;

    /**
     * Рассчитать начисление
     *
     * @param calcStore - хранилище справочников
     * @param klskId    - Klsk Id квартиры
     */
    @Override
    public void genChrg(CalcStore calcStore, Integer klskId) throws WrongParam, ErrorWhileChrg {
        Ko ko = em.find(Ko.class, klskId);
        // получить основной лиц счет
        Kart kartMain = kartMng.getKartMain(ko);
        // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
        Double parVarCntKpr =
                Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"), 0D); // применил NVL, так как много кода
        // в Oracle использовало NVL от NULL параметра...

        ChrgCount chrgCount = new ChrgCount();

        // получить все действующие счетчики квартиры и их объемы
        chrgCount.setLstMeterVol(meterDao.findMeterVolByKlsk(ko.getId(),
                calcStore.getCurDt1(), calcStore.getCurDt2()));
        chrgCount.getLstMeterVol().forEach(t -> {
            log.info("Check2: {}, {}, {}, {}, {}", t.getMeterId(), t.getUslId(), t.getVol(), t.getDtFrom(), t.getDtTo());
        });
        // получить объемы по счетчикам в пропорции на 1 день их работы
        Map<String, BigDecimal> mapDayMeterVol = meterMng.getPartDayMeterVol(chrgCount,
                calcStore);

        // цикл по дням месяца
        Calendar c = Calendar.getInstance();
        for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();
            log.info("Date={}", curDt);
            // получить действующие, отсортированные услуги по квартире (по всем счетам)
            List<Nabor> lstNabor = naborMng.getValidNabor(ko, curDt);

            boolean isExistsMeterColdWater = false;
            boolean isExistsMeterHotWater = false;
            BigDecimal volMeterColdWater = BigDecimal.ZERO;
            BigDecimal volMeterHotWater = BigDecimal.ZERO;

            for (Nabor nabor : lstNabor) {
                if (nabor.getUsl().isMain()) {
                    // по основным услугам
                    log.info("РАСЧЕТ: dt={}, lsk={}, Usl.id={}, fkCalcTp={}, name={}", Utl.getStrFromDate(curDt),
                            nabor.getKart().getLsk(), nabor.getUsl().getId(), nabor.getUsl().getFkCalcTp(), nabor.getUsl().getName());
                    // получить цены по услуге по лицевому счету из набора услуг!
                    DetailUslPrice detailUslPrice = naborMng.getDetailUslPrice(nabor);

                    // получить кол-во проживающих по лицевому счету из набора услуг!
                    CountPers countPers = kartPrMng.getCountPersByDate(kartMain, nabor, parVarCntKpr, curDt);
                    SocStandart socStandart = null;
                    // получить наличие счетчика
                    boolean isMeterExist = false;
                    BigDecimal tempVol = BigDecimal.ZERO;
                    // объемы
                    BigDecimal dayVol = BigDecimal.ZERO;
                    BigDecimal dayVolOverSoc = BigDecimal.ZERO;
                    BigDecimal dayVolEmpty = BigDecimal.ZERO;

                    // площади
                    BigDecimal area = BigDecimal.ZERO;
                    BigDecimal areaOverSoc = BigDecimal.ZERO;
                    BigDecimal areaEmpty = BigDecimal.ZERO;

                    if (Utl.in(nabor.getUsl().getFkCalcTp(), 25)) {
                        // Текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                        area = Utl.nvl(kartMain.getOpl(), BigDecimal.ZERO);
                        dayVol = area.multiply(calcStore.getPartDayMonth());
                        socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                    } else if (Utl.in(nabor.getUsl().getFkCalcTp(), 17, 18)) {
                        // Х.В., Г.В., без уровня соцнормы/свыше
                        // получить объем по нормативу в доле на 1 день
                        // узнать, работал ли хоть один счетчик в данном дне
                        isMeterExist = meterMng.isExistAnyMeter(chrgCount, nabor.getUsl().getId(), curDt);
                        // получить соцнорму
                        socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                        area = Utl.nvl(kartMain.getOpl(), BigDecimal.ZERO);
                        if (isMeterExist) {
                            // для водоотведения
                            if (nabor.getUsl().getFkCalcTp().equals(17)) {
                                isExistsMeterColdWater=true;
                            } else if (nabor.getUsl().getFkCalcTp().equals(18)) {
                                isExistsMeterHotWater=true;
                            }
                            // получить объем по счетчику в пропорции на 1 день его работы
                            tempVol = mapDayMeterVol.get(nabor.getUsl().getId());
                            // в данном случае - объем уже в пропорции на 1 день
                            log.info("uslId={}, dt={}, dayVol={}", nabor.getUsl().getId(), curDt, tempVol);
                        } else {
                            tempVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                            log.info("uslId={}, dt={}, нет счетчика! объем по нормативу={}",
                                    nabor.getUsl().getId(), curDt, tempVol);
                        }
                        if (countPers.isEmpty) {
                            // пустая квартира
                            dayVolEmpty = tempVol;
                        } else {
                            // квартира с проживающими
                            dayVol = tempVol;
                        }
                        // для водоотведения
                        if (nabor.getUsl().getFkCalcTp().equals(17)) {
                            volMeterColdWater=tempVol;
                        } else if (nabor.getUsl().getFkCalcTp().equals(18)) {
                            volMeterHotWater=tempVol;
                        }

                    } else if (Utl.in(nabor.getUsl().getFkCalcTp(), 19)) {
                        // Водоотведение без уровня соцнормы/свыше
                        // получить объем по нормативу в доле на 1 день
                        // узнать, работал ли хоть один счетчик в данном дне

                        // получить соцнорму
                        socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                        area = Utl.nvl(kartMain.getOpl(), BigDecimal.ZERO);

                        if (isExistsMeterColdWater || isExistsMeterHotWater) {
                            isMeterExist=true;
                        }
                        // сложить предварительно рассчитанные объемы х.в.+г.в.
                        tempVol=volMeterColdWater.add(volMeterHotWater);
                        if (countPers.isEmpty) {
                            // пустая квартира
                            dayVolEmpty = tempVol;
                        } else {
                            // квартира с проживающими
                            dayVol = tempVol;
                        }
                    } else if (Utl.in(nabor.getUsl().getFkCalcTp(), 14)) {
                        // Отопление гкал.
                        // получить соцнорму
                        socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                        area = Utl.nvl(kartMain.getOpl(), BigDecimal.ZERO);

                    } else {
                        throw new ErrorWhileChrg("ОШИБКА! По услуге fkCalcTp="+nabor.getUsl().getFkCalcTp()+
                                " не определён блок if в GenChrgProcessMngImpl.genChrg");
                    }

                    //log.info("countpers={}", countPers.isEmpty);
                    // сгруппировать
                    UslPriceVol uslPriceVol = UslPriceVol.UslPriceVolBuilder.anUslPriceVol()
                            .withDtFrom(curDt).withDtTo(curDt).withDtFrom(curDt)
                            .withUsl(nabor.getUsl())
                            .withOrg(nabor.getOrg())
                            .withIsCounter(isMeterExist)
                            .withIsEmpty(countPers.isEmpty)
                            .withSocStdt(socStandart.norm)
                            .withPrice(detailUslPrice.price)
                            .withPriceOverSoc(detailUslPrice.priceOverSoc)
                            .withPriceEmpty(detailUslPrice.priceEmpt)
                            .withVol(dayVol)
                            .withVolOverSoc(dayVolOverSoc)
                            .withVolEmpty(dayVolEmpty)
                            .withArea(area)
                            .withAreaOverSoc(areaOverSoc)
                            .withAreaEmpty(areaEmpty)
                            .withKpr(countPers.kpr).withKprOt(countPers.kprOt).withKprWr(countPers.kprWr)
                            .withPartDayMonth(calcStore.getPartDayMonth())
                            .build();
                    chrgCount.groupUslPriceVol(uslPriceVol);
                }
            }

            // Блок умножения объем на цену (расчет в рублях)
        }

        // получить кол-во проживающих по лиц.счету
        log.info("ИТОГО:");
        log.info("UslPriceVol:");
        for (UslPriceVol t : chrgCount.getLstUslPriceVol()) {
            if (t.usl.getId().equals("013")) {
                log.info("dt1={} dt2={} usl={} org={} cnt={} " +
                                "empt={} stdt={} " +
                                "prc={} prcOvSc={} prcEmpt={} " +
                                "vol={} volOvSc={} volEmpt={} area={} areaOvSc={} " +
                                "areaEmpt={} kpr={} kprOt={} kprWr={}",
                        Utl.getStrFromDate(t.dtFrom), Utl.getStrFromDate(t.dtTo),
                        t.usl.getId(), t.org.getId(), t.isCounter, t.isEmpty,
                        t.socStdt, t.price, t.priceOverSoc, t.priceEmpty,
                        t.vol.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.volOverSoc.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.volEmpty.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.area.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.areaOverSoc.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.areaEmpty.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.kpr.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.kprOt.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.kprWr.setScale(5, BigDecimal.ROUND_HALF_UP));
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
/*
    private void addChrgVol(ChrgCount chrgCount, Usl usl,
                            boolean isCounter,
                            BigDecimal dayPartMonth, CountPers countPers) {
        List<UslOrgPers> lstChrgVol = chrgCount.getMapChrgVol().get(usl);
        if (lstChrgVol == null) {
            lstChrgVol = new ArrayList<>();
            chrgCount.getMapChrgVol().put(usl, lstChrgVol);
        }
        UslOrgPers foundChrgVol = null;
        if (lstChrgVol.size() !=0) {
            // получить записи с такими же ключевыми параметрами - наличия счетчика и пустой квартиры
            lstChrgVol.forEach(t-> log.info("TEST t.isCounter={}, t.isEmpty={}", t.isCounter, t.isEmpty));
            foundChrgVol = lstChrgVol.stream()
                    .filter(t -> t.isCounter == isCounter && t.isEmpty == countPers.isEmpty)
                    .findFirst().orElse(null);
        }
        if (lstChrgVol.size()==0 || foundChrgVol == null) {
            // создать запись
            UslOrgPers chrgVol = new UslOrgPers(usl, countPers.isEmpty, isCounter);
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
*/

}