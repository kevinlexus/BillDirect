package com.dic.app.mm.impl;

import com.dic.app.mm.DistVolMng;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.UslVolKart;
import com.dic.bill.dto.UslVolKartGrp;
import com.dic.bill.dto.UslVolVvod;
import com.dic.bill.mm.ObjParMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Сервис распределения объемов по дому
 * ОДН, и прочие объемы
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
public class DistVolMngImpl implements DistVolMng {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private ProcessMng processMng;
    @Autowired
    private KartDAO kartDAO;
    @Autowired
    private GenChrgProcessMng genChrgProcessMng;
    @Autowired
    private ObjParMng objParMng;

    // норматив по эл.энерг. ОДН в доме без лифта
    private final BigDecimal ODN_EL_NORM = new BigDecimal("2.7");
    // норматив по эл.энерг. ОДН в доме с лифтом
    private final BigDecimal ODN_EL_NORM_WITH_LIFT = new BigDecimal("4.1");

    /**
     * Распределить объемы по вводу (по всем вводам, если reqConf.vvod == null)
     *
     * @param reqConf - параметры запроса
     */
    @Override
    public void distVolByVvod(RequestConfig reqConf) throws ErrorWhileChrgPen, WrongParam, WrongGetMethod {

        // загрузить справочники
        CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);
        Vvod vvod = reqConf.getVvod();
        // тип распределения
        Integer distTp = Utl.nvl(vvod.getDistTp(), 0);
        // использовать счетчики при распределении?
        Boolean isUseSch = Utl.nvl(vvod.getIsUseSch(), false);

        // объем для распределения
        BigDecimal kub = Utl.nvl(vvod.getKub(), BigDecimal.ZERO);
        Usl usl = vvod.getUsl();

        // тип услуги
        int tp = -1;
        if (Utl.in(usl.getFkCalcTp(), 3, 17, 4, 18, 31, 38, 40)) {
            if (Utl.in(usl.getFkCalcTp(), 3, 17, 38)) {
                // х.в.
                tp = 0;
            } else if (Utl.in(usl.getFkCalcTp(), 4, 18, 40)) {
                // г.в.
                tp = 1;
            } else if (Utl.in(usl.getFkCalcTp(), 31)) {
                // эл.эн.
                tp = 2;
            }
        } else {
            // прочие услуги
            tp = 3;
        }

        // СБОР ИНФОРМАЦИИ, для расчета ОДН, подсчета итогов
        // кол-во лиц.счетов, объемы, кол-во прожив.
        // собрать информацию об объемах по лиц.счетам принадлежащим вводу
        processMng.genProcessAll(reqConf, calcStore);

        // объемы по лиц.счетам
        final List<UslVolKart> lstUslVolKart =
                calcStore.getChrgCountAmount().getLstUslVolKart().stream()
                        .filter(t -> t.usl.equals(usl)).collect(Collectors.toList());
        final List<UslVolKartGrp> lstUslVolKartGrp =
                calcStore.getChrgCountAmount().getLstUslVolKartGrp().stream()
                        .filter(t -> t.usl.equals(usl)).collect(Collectors.toList());


        log.info("Объемы по лиц.счетам, вводу:");
        for (UslVolKart t : calcStore.getChrgCountAmount().getLstUslVolKart()) {
            if (Utl.in(t.usl.getId(), "011")) {
                log.info("lsk={} usl={} cnt={} " +
                                "empt={} resid={} " +
                                "vol={} area={} kpr={}",
                        t.kart.getLsk(),
                        t.usl.getId(), t.isMeter, t.isEmpty, t.isResidental,
                        t.vol.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.area.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.kpr.setScale(5, BigDecimal.ROUND_HALF_UP));
            }
        }

        // ОЧИСТКА информации ОДН
        clearODN(vvod);

        // ПОЛУЧИТЬ итоговые объемы по вводу
        if (Utl.in(tp, 0, 1, 2)) {
            // х.в. г.в. эл.эн.
            for (UslVolKart t : lstUslVolKart) {
                // сохранить объемы по вводу для статистики
                if (t.isResidental) {
                    // по жилым помещениям
                    if (t.isMeter) {
                        // по счетчикам
                        // объем
                        vvod.setKubSch(vvod.getKubSch().add(t.vol));
                        // кол-во лицевых
                        vvod.setSchCnt(vvod.getSchCnt().add(new BigDecimal("1")));
                        // кол-во проживающих
                        vvod.setSchKpr(vvod.getSchKpr().add(t.kpr));
                    } else {
                        // по нормативам
                        // объем
                        vvod.setKubNorm(vvod.getKubNorm().add(t.vol));
                        // кол-во лицевых
                        vvod.setCntLsk(vvod.getCntLsk().add(new BigDecimal("1")));
                        // кол-во проживающих
                        vvod.setKpr(vvod.getKpr().add(t.kpr));
                    }

                } else {
                    // по нежилым помещениям
                    // площадь
                    vvod.setOplAr(vvod.getOplAr().add(t.area));
                    // объем
                    vvod.setKubAr(vvod.getKubAr().add(t.vol));
                }

                // получить совокупный объем
                UslVolKartGrp uslVolKartGrp = getUslVolKartGrp(calcStore, t);

                // учитывать ли объем
                boolean isCountVol = getIsCountVol(distTp, isUseSch, uslVolKartGrp);

                // площадь по вводу
                if (isCountVol) {
                    vvod.setOplAdd(Utl.nvl(vvod.getOplAdd(), BigDecimal.ZERO).add(t.area));
                }
            }
        } else if (usl.getFkCalcTp().equals(14)) {
            // Отопление Гкал
            for (UslVolVvod t : calcStore.getChrgCountAmount().getLstUslVolVvod()) {
                //log.info("usl={}, cnt={}, empt={}, resid={}, t.vol={}, t.area={}",
                //        t.usl.getId(), t.isMeter, t.isEmpty, t.isResidental, t.vol, t.area);
                if (!t.isResidental) {
                    // сохранить объемы по вводу для статистики
                    // площадь по нежилым помещениям
                    vvod.setOplAr(Utl.nvl(vvod.getOplAr(), BigDecimal.ZERO).add(t.area));
                }
                // площадь по вводу
                vvod.setOplAdd(Utl.nvl(vvod.getOplAdd(), BigDecimal.ZERO).add(t.area));
            }
            // округлить площади по вводу
            vvod.setOplAr(vvod.getOplAr().setScale(5, RoundingMode.HALF_UP));
            vvod.setOplAdd(vvod.getOplAdd().setScale(5, RoundingMode.HALF_UP));
        }


        // Итоги
        // объем
        BigDecimal volAmnt = vvod.getKubNorm().add(vvod.getKubSch()).add(vvod.getKubAr());
        // кол-во проживающих
        BigDecimal kprAmnt = vvod.getKpr().add(vvod.getSchKpr());
        // площадь по вводу
        BigDecimal areaVvod = vvod.getOplAdd();

        log.info("*** Ввод id={}, услуга usl={}, площадь={}, кол-во прож.={}, объем={}, введено={}",
                vvod.getId(), vvod.getUsl().getId(), areaVvod, kprAmnt, volAmnt, kub);

        BigDecimal amntKprDet = lstUslVolKart.stream().map(t -> t.kpr).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal amntKprGrp = lstUslVolKartGrp.stream().map(t -> t.kpr).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal amntVolDet = lstUslVolKart.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal amntVolGrp = lstUslVolKartGrp.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal amntAreaDet = lstUslVolKart.stream().map(t -> t.area).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal amntAreaGrp = lstUslVolKartGrp.stream().map(t -> t.area).reduce(BigDecimal.ZERO, BigDecimal::add);

        // ОГРАНИЧЕНИЕ распределения по законодательству
        LimitODN limitODN = calcLimit(vvod.getHouse().getKo(), tp, kprAmnt, areaVvod);


        // РАСПРЕДЕЛЕНИЕ
        if (!kub.equals(BigDecimal.ZERO)) {
            if (Utl.in(distTp, 1, 3)) {
                BigDecimal diff = kub.subtract(volAmnt);
                if (diff.compareTo(BigDecimal.ZERO) != 0 && !areaVvod.equals(BigDecimal.ZERO)) {
                    // есть небаланс
                    if (diff.compareTo(BigDecimal.ZERO) > 0) {
                        log.info("*** перерасход={}", diff);
                        // ПЕРЕРАСХОД
                        // доначисление пропорционально площади (в т.ч.арендаторы), если небаланс > 0
                        for (UslVolKartGrp t : lstUslVolKartGrp) {
                            Nabor naborChild = t.kart.getNabor().stream()
                                    .filter(d -> d.getUsl().equals(t.usl.getUslChild()))
                                    .findAny().orElse(null);

                            // учитывать ли объем
                            boolean isCountVol = getIsCountVol(distTp, isUseSch, t);
                            if (naborChild != null && isCountVol) {
                                // рассчитать долю объема
                                BigDecimal proc = t.area.divide(areaVvod, 20, BigDecimal.ROUND_HALF_UP);
                                BigDecimal volDist = proc.multiply(diff).setScale(5, BigDecimal.ROUND_HALF_UP);
                                naborChild.setVolAdd(volDist);
                                // лимит (информационно)
                                BigDecimal limit = null;
                                if (Utl.in(tp, 0, 1)) {
                                    // х.в. г.в.
                                    limit = limitODN.limitArea.multiply(t.area);
                                } else if (tp == 2) {
                                    // эл.эн.
                                    limit = limitODN.odnNorm.multiply(limitODN.areaProp) // взято из P_VVOD строка 591
                                            .multiply(t.area).divide(areaVvod, 20, BigDecimal.ROUND_HALF_UP);
                                }
                                naborChild.setLimit(limit);
                                // добавить инфу по ОДН.
                                if (!volDist.equals(BigDecimal.ZERO)) {
                                    Charge charge = new Charge();
                                    t.kart.getCharge().add(charge);
                                    charge.setKart(t.kart);
                                    charge.setUsl(t.usl.getUslChild());
                                    charge.setTestOpl(volDist);
                                    charge.setType(5);
                                }
                            }
                        }
                    } else {
                        // ЭКОНОМИЯ - рассчитывается пропорционально кол-во проживающих, кроме Нежилых
                        if (!kprAmnt.equals(BigDecimal.ZERO)) {
                            BigDecimal diffPerPers = diff.divide(kprAmnt, 20, BigDecimal.ROUND_HALF_UP).abs();
                            log.info("*** экономия={}, на 1 прожив={}", diff.abs(), diffPerPers);

                            // лиц.счет, объем, лимит
                            // по счетчику
                            Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistMeterVol = new HashMap<>();
                            // по нормативу
                            Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistNormVol = new HashMap<>();
                            // общий
                            Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistVol = new HashMap<>();

                            BigDecimal volSum = BigDecimal.ZERO;
                            for (UslVolKart t : lstUslVolKart) {
                                if (t.isResidental) {
                                    // кроме нежилых
                                    Nabor naborChild = t.kart.getNabor().stream()
                                            .filter(d -> d.getUsl().equals(t.usl.getUslChild()))
                                            .findAny().orElse(null);

                                    // получить совокупный объем
                                    UslVolKartGrp uslVolKartGrp = getUslVolKartGrp(calcStore, t);
                                    // учитывать ли объем
                                    boolean isCountVol = getIsCountVol(distTp, isUseSch, uslVolKartGrp);
                                    if (naborChild != null && isCountVol) {
                                        // рассчитать долю объема на лиц.счет
                                        BigDecimal volDistKart = uslVolKartGrp.kpr.multiply(diffPerPers);
                                        // ограничить объем экономии текущим общим объемом норматив+счетчик
                                        if (volDistKart.compareTo(uslVolKartGrp.vol) > 0) {
                                            volDistKart = uslVolKartGrp.vol;
                                        }
                                        log.info("lsk={}, kpr={}, собств.объем={}, к распр={}",
                                                t.kart.getLsk(), t.kpr, t.vol, volDistKart);

                                        // лимит (информационно)
                                        BigDecimal limit = null;
                                        if (Utl.in(tp, 0, 1)) {
                                            // х.в. г.в.
                                            limit = limitODN.limitArea.multiply(uslVolKartGrp.area);
                                        } else if (tp == 2) {
                                            // эл.эн.
                                            limit = limitODN.odnNorm.multiply(limitODN.areaProp) // взято из P_VVOD строка 591
                                                    .multiply(uslVolKartGrp.area).divide(areaVvod, 20, BigDecimal.ROUND_HALF_UP);
                                        }

                                        // установить лимит (не эффективно, по одним и тем же записям - много раз)
                                        naborChild.setLimit(limit);

                                        if (!volDistKart.equals(BigDecimal.ZERO)) {
                                            // рассчитать долю объема на данную информационную строку UslVolKart
                                            BigDecimal proc = t.vol.divide(uslVolKartGrp.vol, 20, BigDecimal.ROUND_HALF_UP);
                                            BigDecimal vol = proc.multiply(volDistKart);
                                            // добавить объем
                                            if (t.isMeter) {
                                                addDistVol(mapDistMeterVol, mapDistVol, t, limit, vol);
                                            } else {
                                                addDistVol(mapDistNormVol, mapDistVol, t, limit, vol);
                                            }
                                            log.info("доля распр:={}", vol);
                                            volSum = volSum.add(vol);
                                            log.info("итог распр:={}", volSum);

                                        }
                                    }
                                }
                            }

                            log.info("");
                            // добавить объем экономии и инфу по ОДН. по нормативу и счетчику
                            mapDistMeterVol.entrySet().forEach(t -> addChargePrep(usl, t, true));
                            mapDistNormVol.entrySet().forEach(t -> addChargePrep(usl, t, false));
                            mapDistVol.entrySet().forEach(t -> addCharge(usl, t, false));
                        }
                    }
                }

            } else if (usl.getFkCalcTp().equals(14)) {
                // Отопление Гкал, распределить по площади
                if (!areaVvod.equals(BigDecimal.ZERO)) {
                    for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
                        BigDecimal volForDist = kub.setScale(5, RoundingMode.HALF_UP);
                        BigDecimal vol = volForDist.multiply(t.area.divide(areaVvod, 5, RoundingMode.HALF_UP))
                                .setScale(5, RoundingMode.HALF_UP);
                        for (Nabor nabor : t.kart.getNabor()) {
                            if (nabor.getUsl().equals(usl)) {
                                nabor.setVol(vol);
                                log.info("Распределено: lsk={}, usl={}, kub={}, vol={}, area={}, areaVvod={}",
                                        t.kart.getLsk(), usl.getId(), kub, vol, t.area, areaVvod);
                            }
                        }
                    }
                }
            }
        }

        //log.info("Итоговые объемы по вводу:");
        //log.info("oplAdd={}, oplAr={}", vvod.getOplAdd(), vvod.getOplAr());

        // РАСПРЕДЕЛИТЬ объемы в домах с ОДПУ


        // РАСПРЕДЕЛИТЬ объемы в домах без ОДПУ

    }

    /**
     * Добавить информацию распределения объема
     *
     * @param usl          - услуга
     * @param entry        - информационная строка
     * @param isExistMeter - наличие счетчика
     */
    private void addChargePrep(Usl usl, Map.Entry<Kart, Pair<BigDecimal, BigDecimal>> entry, boolean isExistMeter) {
        Kart kart = entry.getKey();
        Pair<BigDecimal, BigDecimal> mapVal = entry.getValue();
        BigDecimal vol = mapVal.getValue0().setScale(3, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal("-1"));

        ChargePrep chargePrep = new ChargePrep();
        kart.getChargePrep().add(chargePrep);
        chargePrep.setKart(kart);
        chargePrep.setUsl(usl);
        chargePrep.setVol(vol);
        chargePrep.setTp(4);
        chargePrep.setExistMeter(isExistMeter);
    }

    /**
     * Добавить информацию распределения объема
     *
     * @param usl          - услуга
     * @param entry        - информационная строка
     * @param isExistMeter - наличие счетчика
     */
    private void addCharge(Usl usl, Map.Entry<Kart, Pair<BigDecimal, BigDecimal>> entry, boolean isExistMeter) {
        Kart kart = entry.getKey();
        Pair<BigDecimal, BigDecimal> mapVal = entry.getValue();
        BigDecimal vol = mapVal.getValue0().setScale(3, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal("-1"));

        Charge charge = new Charge();
        kart.getCharge().add(charge);
        charge.setKart(kart);
        charge.setUsl(usl.getUslChild());
        charge.setTestOpl(vol);
        charge.setType(5);
    }

    /**
     * Сгруппировать распределенные объемы
     *
     * @param mapDistDetVol - распред.объемы по нормативу/счетчику
     * @param mapDistVol    - распред.объемы в совокупности
     * @param t             - объемы по лиц.счету
     * @param limit         - лимит ОДН
     * @param vol           - объем распределения
     */
    private void addDistVol(Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistDetVol,
                            Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistVol,
                            UslVolKart t, BigDecimal limit, BigDecimal vol) {
        Pair<BigDecimal, BigDecimal> mapVal = mapDistDetVol.get(t.kart);
        if (mapVal == null) {
            mapDistDetVol.put(t.kart,
                    Pair.with(vol, limit));
        } else {
            mapVal = mapDistDetVol.get(t.kart);
            mapDistDetVol.put(t.kart,
                    Pair.with(mapVal.getValue0().add(vol), limit));
        }

        mapVal = mapDistVol.get(t.kart);
        if (mapVal == null) {
            mapDistVol.put(t.kart,
                    Pair.with(vol, limit));
        } else {
            mapVal = mapDistVol.get(t.kart);
            mapDistVol.put(t.kart,
                    Pair.with(mapVal.getValue0().add(vol), limit));
        }

    }

    /**
     * Учитывать ли объем?
     *
     * @param distTp        - тип распределения
     * @param isUseSch      - учитывать счетчики?
     * @param uslVolKartGrp - запись объема сгруппированная
     * @return учитывать ли объем?
     */
    private boolean getIsCountVol(Integer distTp, Boolean isUseSch, UslVolKartGrp uslVolKartGrp) {
        boolean isCountVol = true;
        if (!distTp.equals(3) && !isUseSch) {
            // тип распр.<>3 контролировать или нет наличие счетчиков
            isCountVol = uslVolKartGrp.isExistMeterCurrPeriod;
        } else if (distTp.equals(3)) {
            // тип распр.=3 то либо арендатор, либо должен кто-то быть прописан
            isCountVol = !uslVolKartGrp.kart.isResidental() || uslVolKartGrp.isExistPersCurrPeriod;
        }
        return isCountVol;
    }


    /**
     * получить совокупные объемы по лиц счету
     *
     * @param calcStore - хранилище данных
     * @param t         - запись объема по услуге в лиц.счете
     * @return совокупный объем
     */
    private UslVolKartGrp getUslVolKartGrp(CalcStore calcStore, UslVolKart t) {
        return calcStore.getChrgCountAmount().getLstUslVolKartGrp().stream()
                .filter(d -> d.kart.equals(t.kart) && d.usl.equals(t.usl))
                .findAny().orElse(null);
    }

    /**
     * Очистка распределенных объемов
     *
     * @param vvod - ввод
     */
    private void clearODN(Vvod vvod) {
        // почистить нормативы (ограничения)
        log.info("Очистка информации usl={}", vvod.getUsl().getId());
        vvod.setNrm(BigDecimal.ZERO);
        vvod.setCntLsk(BigDecimal.ZERO);
        vvod.setSchCnt(BigDecimal.ZERO);

        vvod.setOplAr(BigDecimal.ZERO);
        vvod.setOplAdd(BigDecimal.ZERO);

        vvod.setKubNorm(BigDecimal.ZERO);
        vvod.setKubSch(BigDecimal.ZERO);
        vvod.setKubAr(BigDecimal.ZERO);

        vvod.setKpr(BigDecimal.ZERO);
        vvod.setSchKpr(BigDecimal.ZERO);

        for (Nabor nabor : vvod.getNabor()) {
            // удалить информацию по корректировкам ОДН
            if (nabor.getUsl().equals(vvod.getUsl())) {
                nabor.getKart().getChargePrep().removeIf(chargePrep -> chargePrep.getUsl().equals(vvod.getUsl())
                        && chargePrep.getTp().equals(4));

                // удалить по зависимым услугам
                nabor.getKart().getCharge().removeIf(charge -> charge.getUsl().equals(vvod.getUsl().getUslChild())
                        && charge.getType().equals(5));

                // занулить по вводу-услуге
                nabor.setVol(null);
                nabor.setVolAdd(null);
                nabor.setLimit(null);

            }

            for (Nabor nabor2 : nabor.getKart().getNabor()) {
                if (nabor2.getUsl().equals(vvod.getUsl().getUslChild())) {
                    // занулить по зависимым услугам
                    nabor2.setVol(null);
                    nabor2.setVolAdd(null);
                    nabor2.setLimit(null);
                }
            }
        }
    }

    /**
     * Рассчитать лимиты распределения по законодательству
     *
     * @param houseKo - Ko дома
     * @param tp      - тип услуги (0 - х.в., 1- г.в., 2 - эл.эн.)
     * @param cntKpr  - кол во прожив. по вводу
     * @param area    - площадь по вводу
     */
    private LimitODN calcLimit(Ko houseKo, int tp, BigDecimal cntKpr, BigDecimal area) throws WrongParam, WrongGetMethod {
        LimitODN limitODN = new LimitODN();

        if (Utl.in(tp, 0, 1)) {
            // х.в. г.в.
            //расчитать лимит распределения
            //если кол-во прожив. > 0
            if (!cntKpr.equals(BigDecimal.ZERO)) {
                // площадь на одного проживающего
                final BigDecimal oplMan = area.divide(cntKpr, 5, BigDecimal.ROUND_HALF_UP);
                final BigDecimal oplLiter = oplLiter(oplMan.intValue());
                limitODN.limitVol = oplLiter
                        .divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP);
                limitODN.odnNorm = limitODN.limitVol;
                final BigDecimal limitArea = oplLiter
                        .divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP);

            }

        } else if (tp == 2) {
            // эл.эн.
            // площадь общ.имущ., норматив, объем на площадь
            limitODN.areaProp = Utl.nvl(objParMng.getBd(houseKo, "area_general_property"), BigDecimal.ZERO);
            BigDecimal existLift = Utl.nvl(objParMng.getBd(houseKo, "exist_lift"), BigDecimal.ZERO);

            if (existLift.equals(BigDecimal.ZERO)) {
                // дом без лифта
                limitODN.odnNorm = ODN_EL_NORM;
            } else {
                // дом с лифтом
                limitODN.odnNorm = ODN_EL_NORM_WITH_LIFT;
            }
        }

        return limitODN;
    }

    /**
     * Распределить объемы в домах с ОДПУ
     *
     * @param reqConf   - параметры запроса
     * @param calcStore - хранилище справочников
     */
    private void distVolWithODPU(RequestConfig reqConf, CalcStore calcStore) {


    }

    /**
     * Распределить объемы в домах без ОДПУ
     *
     * @param vvod
     * @param isMultiThreads
     */
    private void distVolWithoutODPU(Vvod vvod, boolean isMultiThreads) {

    }


    /**
     * таблица для возврата норматива потребления (в литрах) по соотв.площади на человека
     *
     * @param oplMan - площадь на человека
     * @return - норматив потребления
     */
    private BigDecimal oplLiter(int oplMan) {
        double val;
        switch (oplMan) {
            case 1:
                val = 2;
                break;
            case 2:
                val = 2;
                break;
            case 3:
                val = 2;
                break;
            case 4:
                val = 10;
                break;
            case 5:
                val = 10;
                break;
            case 6:
                val = 10;
                break;
            case 7:
                val = 10;
                break;
            case 8:
                val = 10;
                break;
            case 9:
                val = 10;
                break;
            case 10:
                val = 9;
                break;
            case 11:
                val = 8.2;
                break;
            case 12:
                val = 7.5;
                break;
            case 13:
                val = 6.9;
                break;
            case 14:
                val = 6.4;
                break;
            case 15:
                val = 6.0;
                break;
            case 16:
                val = 5.6;
                break;
            case 17:
                val = 5.3;
                break;
            case 18:
                val = 5.0;
                break;
            case 19:
                val = 4.7;
                break;
            case 20:
                val = 4.5;
                break;
            case 21:
                val = 4.3;
                break;
            case 22:
                val = 4.1;
                break;
            case 23:
                val = 3.9;
                break;
            case 24:
                val = 3.8;
                break;
            case 25:
                val = 3.6;
                break;
            case 26:
                val = 3.5;
                break;
            case 27:
                val = 3.3;
                break;
            case 28:
                val = 3.2;
                break;
            case 29:
                val = 3.1;
                break;
            case 30:
                val = 3.0;
                break;
            case 31:
                val = 2.9;
                break;
            case 32:
                val = 2.8;
                break;
            case 33:
                val = 2.7;
                break;
            case 34:
                val = 2.6;
                break;
            case 35:
                val = 2.6;
                break;
            case 36:
                val = 2.5;
                break;
            case 37:
                val = 2.4;
                break;
            case 38:
                val = 2.4;
                break;
            case 39:
                val = 2.3;
                break;
            case 40:
                val = 2.3;
                break;
            case 41:
                val = 2.2;
                break;
            case 42:
                val = 2.1;
                break;
            case 43:
                val = 2.1;
                break;
            case 44:
                val = 2;
                break;
            case 45:
                val = 2;
                break;
            case 46:
                val = 2;
                break;
            case 47:
                val = 1.9;
                break;
            case 48:
                val = 1.9;
                break;
            case 49:
                val = 1.8;
                break;
            default:
                val = 1.8;

        }

        return BigDecimal.valueOf(val);
    }

    /**
     * DTO для хранения лимитов ОДН
     */
    class LimitODN {
        BigDecimal odnNorm = BigDecimal.ZERO;
        // допустимый лимит ОДН по законодательству (общий)
        BigDecimal limitVol = BigDecimal.ZERO;
        // допустимый лимит ОДН на 1 м2
        BigDecimal limitArea = BigDecimal.ZERO;
        // площадь общего имущества
        BigDecimal areaProp;
    }

}