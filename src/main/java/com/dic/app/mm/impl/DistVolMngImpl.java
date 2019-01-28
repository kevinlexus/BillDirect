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
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.ErrorWhileDist;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    /**
     * Распределить объемы по вводу (по всем вводам, если reqConf.vvod == null)
     *
     * @param reqConf   - параметры запроса
     * @param calcStore - хранилище справочников, объемов начисления
     */
    @Override
    public void distVolByVvod(RequestConfig reqConf, CalcStore calcStore) throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist {

        Vvod vvod = reqConf.getVvod();
        // тип распределения
        final Integer distTp = Utl.nvl(vvod.getDistTp(), 0);
        // использовать счетчики при распределении?
        final Boolean isUseSch = Utl.nvl(vvod.getIsUseSch(), false);

        // объем для распределения
        final BigDecimal kub = Utl.nvl(vvod.getKub(), BigDecimal.ZERO).setScale(5, BigDecimal.ROUND_HALF_UP);
        final Usl usl = vvod.getUsl();
        // неограничивать лимитом потребления ОДН?
        final boolean isWithoutLimit = Utl.nvl(vvod.getIsWithoutLimit(), false);

        // тип услуги
        int tpTmp = -1;

        // вид услуги ограничения ОДН

        if (Utl.in(usl.getFkCalcTp(), 3, 17, 4, 18, 31, 38, 40)) {
            if (Utl.in(usl.getFkCalcTp(), 3, 17, 38, 4, 18, 40)) {
                // х.в., г.в.
                tpTmp = 0;
            } else if (Utl.in(usl.getFkCalcTp(), 31)) {
                // эл.эн.
                tpTmp = 2;
            }
        } else if (Utl.in(usl.getFkCalcTp(), 14, 23)) {
            // прочая услуга
            tpTmp = 3;
        } else if (Utl.in(usl.getFkCalcTp(), 11, 15)) {
            // остальные услуги
            tpTmp = 4;
        }
        final int tp = tpTmp;

        if (tp != -1) {
            // ОЧИСТКА информации ОДН
            clearODN(vvod);

            // СБОР ИНФОРМАЦИИ, для расчета ОДН, подсчета итогов
            // кол-во лиц.счетов, объемы, кол-во прожив.
            // собрать информацию об объемах по лиц.счетам принадлежащим вводу
            processMng.genProcessAll(reqConf, calcStore);

            // объемы по лиц.счетам (базовый фильтр по услуге)
            final List<UslVolKart> lstUslVolKartBase =
                    calcStore.getChrgCountAmount().getLstUslVolKart().stream()
                            .filter(t -> t.usl.equals(usl)).collect(Collectors.toList());
            final List<UslVolKartGrp> lstUslVolKartGrpBase =
                    calcStore.getChrgCountAmount().getLstUslVolKartGrp().stream()
                            .filter(t -> t.usl.equals(usl)).collect(Collectors.toList());

            // ПОЛУЧИТЬ итоговые объемы по вводу
            List<UslVolKartGrp> lstUslVolKartGrp;
            if (Utl.in(tp, 0, 2)) {
                // х.в., г.в., эл.эн., эл.эн.ОДН
                lstUslVolKartGrp =
                        lstUslVolKartGrpBase.stream()
                                .filter(t->
                                        t.kart.getNabor().stream()
                                        .anyMatch((d -> d.getUsl().equals(t.usl.getUslChild()))) // где есть наборы по дочерним усл.
                                        && getIsCountOpl(tp, distTp, isUseSch, t)).collect(Collectors.toList());

                for (UslVolKartGrp uslVolKartGrp : lstUslVolKartGrp) {
                    // сохранить объемы по вводу для статистики
                    if (uslVolKartGrp.isResidental) {
                        // по жилым помещениям
                        if (uslVolKartGrp.isExistMeterCurrPeriod) {
                            // по счетчикам
                            // объем
                            vvod.setKubSch(vvod.getKubSch().add(uslVolKartGrp.vol));
                            // кол-во лицевых
                            vvod.setSchCnt(vvod.getSchCnt().add(new BigDecimal("1")));
                            // кол-во проживающих
                            vvod.setSchKpr(vvod.getSchKpr().add(uslVolKartGrp.kpr));
                        } else {
                            // по нормативам
                            // объем
                            vvod.setKubNorm(vvod.getKubNorm().add(uslVolKartGrp.vol));
                            // кол-во лицевых
                            vvod.setCntLsk(vvod.getCntLsk().add(new BigDecimal("1")));
                            // кол-во проживающих
                            vvod.setKpr(vvod.getKpr().add(uslVolKartGrp.kpr));
                        }

                    } else {
                        // по нежилым помещениям
                        // площадь
                        vvod.setOplAr(vvod.getOplAr().add(uslVolKartGrp.area));
                        // объем
                        vvod.setKubAr(vvod.getKubAr().add(uslVolKartGrp.vol));
                        if (uslVolKartGrp.isExistMeterCurrPeriod) {
                            // по счетчикам
                            // кол-во лицевых
                            vvod.setSchCnt(vvod.getSchCnt().add(new BigDecimal("1")));
                        } else {
                            // по нормативам ??? здесь только счетчики должны быть!
                            // кол-во лицевых
                            vvod.setCntLsk(vvod.getCntLsk().add(new BigDecimal("1")));
                        }
                    }
                    // итоговая площадь по вводу
                    vvod.setOplAdd(Utl.nvl(vvod.getOplAdd(), BigDecimal.ZERO).add(uslVolKartGrp.area));
                }
            } else if (tp == 3) {
                // Отопление Гкал
                for (UslVolKart t : lstUslVolKartBase) {
                    //log.trace("usl={}, cnt={}, empt={}, resid={}, t.vol={}, t.area={}",
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
            // весь объем
            BigDecimal volAmnt = vvod.getKubNorm().add(vvod.getKubSch()).add(vvod.getKubAr());
            // объем кроме арендаторов
            BigDecimal volAmntResident = vvod.getKubNorm().add(vvod.getKubSch());
            // кол-во проживающих
            BigDecimal kprAmnt = vvod.getKpr().add(vvod.getSchKpr());
            // площадь по вводу, варьируется от услуги
            BigDecimal areaAmnt = vvod.getOplAdd();
            // кол-во лиц.счетов по счетчикам
            BigDecimal cntSchAmnt = vvod.getSchCnt();
            // кол-во лиц.счетов по нормативам
            BigDecimal cntNormAmnt = vvod.getCntLsk();

            log.info("*** Ввод id={}, услуга usl={}, площадь={}, кол-во лиц сч.={}, кол-во лиц норм.={}, кол-во прож.={}, объем={}," +
                            " объем за искл.аренд.={},  введено={}",
                    vvod.getId(), vvod.getUsl().getId(), areaAmnt, cntSchAmnt, cntNormAmnt, kprAmnt, volAmnt, volAmntResident, kub);

/*
            BigDecimal amntKprDet = lstUslVolKart.stream().map(t -> t.kpr).reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal amntKprGrp = lstUslVolKartGrp.stream().map(t -> t.kpr).reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal amntVolDet = lstUslVolKart.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal amntVolGrp = lstUslVolKartGrp.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal amntAreaDet = lstUslVolKart.stream().map(t -> t.area).reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal amntAreaGrp = lstUslVolKartGrp.stream().map(t -> t.area).reduce(BigDecimal.ZERO, BigDecimal::add);
*/

            // ОГРАНИЧЕНИЕ распределения по законодательству
            final LimitODN limitODN = calcLimit(vvod.getHouse().getKo(), tp, kprAmnt, areaAmnt);

            // РАСПРЕДЕЛЕНИЕ
            if (kub.compareTo(BigDecimal.ZERO) != 0) {
                if (Utl.in(usl.getFkCalcTp(), 3, 17, 4, 18, 31, 38, 40)) {
                    if (Utl.in(distTp, 1, 3)) {
                        BigDecimal diff = kub.subtract(volAmnt);
                        BigDecimal diffDist = diff.abs();
                        if (diff.compareTo(BigDecimal.ZERO) != 0 && areaAmnt.compareTo(BigDecimal.ZERO) != 0) {
                            // выборка для распределения
                            // есть небаланс
                            if (diff.compareTo(BigDecimal.ZERO) > 0) {
                                // ПЕРЕРАСХОД
                                log.info("*** перерасход={}", diff);
                                // доначисление пропорционально площади (в т.ч.арендаторы), если небаланс > 0
                                lstUslVolKartGrp =
                                        lstUslVolKartGrpBase.stream()
                                                .filter(t -> t.usl.equals(usl) &&
                                                        t.kart.getNabor().stream()
                                                                .anyMatch((d -> d.getUsl().equals(t.usl.getUslChild()))) // где есть наборы по дочерним усл.
                                                        && getIsCountOpl(tp, distTp, isUseSch, t)).collect(Collectors.toList());
                                Iterator<UslVolKartGrp> iter = lstUslVolKartGrp.iterator();
                                while (iter.hasNext()) {
                                    UslVolKartGrp t = iter.next();
                                    // по дочерним услугам
                                    // рассчитать долю объема
                                    BigDecimal proc = t.area.divide(areaAmnt, 20, BigDecimal.ROUND_HALF_UP);
                                    BigDecimal volDist;
                                    if (iter.hasNext()) {
                                        volDist = proc.multiply(diff).setScale(3, BigDecimal.ROUND_HALF_UP);
                                    } else {
                                        // остаток объема, в т.ч. округление
                                        volDist = diffDist;
                                    }
                                    diffDist = diffDist.subtract(volDist);

                                    // лимит (информационно)
                                    BigDecimal limitTmp = null;
                                    if (Utl.in(tp, 0)) {
                                        // х.в. г.в.
                                        limitTmp = limitODN.limitArea.multiply(t.area);
                                    } else if (tp == 2) {
                                        // эл.эн.
                                        limitTmp = limitODN.amntVolODN // взято из P_VVOD строка 591
                                                .multiply(t.area).divide(areaAmnt, 20, BigDecimal.ROUND_HALF_UP);
                                    }
                                    BigDecimal limit = limitTmp;
                                    t.kart.getNabor().stream()
                                            .filter(d -> d.getUsl().equals(t.usl.getUslChild()))
                                            .findFirst().ifPresent(d -> {
                                        log.info("Перерасход lsk={}, usl={}, vol={}",
                                                d.getKart().getLsk(), d.getUsl().getId(), volDist);
                                        d.setLimit(limit);
                                        d.setVolAdd(volDist);
                                    });
                                    // добавить инфу по ОДН.
                                    if (volDist.compareTo(BigDecimal.ZERO) != 0) {
                                        Charge charge = new Charge();
                                        t.kart.getCharge().add(charge);
                                        charge.setKart(t.kart);
                                        charge.setUsl(t.usl.getUslChild());
                                        charge.setTestOpl(volDist);
                                        charge.setType(5);
                                    }
                                }
                            } else {
                                // ЭКОНОМИЯ - рассчитывается пропорционально кол-во проживающих, кроме Нежилых
                                // считается без ОКРУГЛЕНИЯ, так как экономия может быть срезана текущим объемом!
                                if (kprAmnt.compareTo(BigDecimal.ZERO) != 0) {
                                    BigDecimal diffPerPers = diffDist.divide(kprAmnt, 20, BigDecimal.ROUND_HALF_UP);
                                    log.info("*** экономия={}, на 1 прожив={}", diffDist, diffPerPers);
                                    // лиц.счет, объем, лимит
                                    // по счетчику
                                    Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistMeterVol = new HashMap<>();
                                    // по нормативу
                                    Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistNormVol = new HashMap<>();
                                    // общий
                                    Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistVol = new HashMap<>();
                                    boolean isRestricted = false;
                                    lstUslVolKartGrp =
                                            lstUslVolKartGrpBase.stream()
                                                    .filter(t -> t.isResidental && // здесь только жилые помещения
                                                            t.kart.getNabor().stream()
                                                                    .anyMatch((d -> d.getUsl().equals(t.usl.getUslChild()))) // где есть наборы по дочерним усл.
                                                            && getIsCountOpl(tp, distTp, isUseSch, t)).collect(Collectors.toList());

                                    Iterator<UslVolKartGrp> iter = lstUslVolKartGrp.iterator();
                                    while (iter.hasNext()) {
                                        UslVolKartGrp uslVolKartGrp = iter.next();

                                        // лимит (информационно)
                                        BigDecimal limitTmp = null;
                                        if (Utl.in(tp, 0)) {
                                            // х.в. г.в.
                                            limitTmp = limitODN.limitArea.multiply(uslVolKartGrp.area);
                                        } else if (tp == 2) {
                                            // эл.эн.
                                            limitTmp = limitODN.amntVolODN // взято из P_VVOD строка 591
                                                    .multiply(uslVolKartGrp.area).divide(areaAmnt, 3, BigDecimal.ROUND_HALF_UP);
                                        }
                                        BigDecimal limit = limitTmp;
                                        // установить лимит
                                        uslVolKartGrp.kart.getNabor().stream()
                                                .filter(d -> d.getUsl().equals(uslVolKartGrp.usl.getUslChild()))
                                                .findFirst().ifPresent(d -> d.setLimit(limit));

                                        // распределить экономию в доле на кол-во проживающих
                                        List<UslVolKart> lstUslVolKart2 = lstUslVolKartBase.stream()
                                                .filter(t -> uslVolKartGrp.kart.equals(t.kart) && !t.isEmpty)
                                                .collect(Collectors.toList());
                                        Iterator<UslVolKart> iter2 = lstUslVolKart2.iterator();
                                        while (iter2.hasNext()) {
                                            UslVolKart uslVolKart = iter2.next();
                                            BigDecimal volDist = uslVolKart.kpr.multiply(diffPerPers)
                                                    .setScale(5, BigDecimal.ROUND_HALF_UP);
                                            // ограничить объем экономии текущим общим объемом норматив+счетчик
                                            if (volDist.compareTo(uslVolKart.vol) > 0) {
                                                log.info("ОГРАНИЧЕНИЕ экономии: lsk={}, vol={}",
                                                        uslVolKart.kart.getLsk(),
                                                        volDist.subtract(uslVolKart.vol));
                                                isRestricted = true;
                                                volDist = uslVolKart.vol;
                                            }

                                            diffDist = diffDist.subtract(volDist);
                                            if (!iter.hasNext() && !iter2.hasNext() && !isRestricted) {
                                                // остаток на последнюю строку, если не было ограничений экономии
                                                if (diffDist.abs().compareTo(new BigDecimal("0.05")) > 0) {
                                                    throw new ErrorWhileDist("ОШИБКА! Некорректный объем округления, " +
                                                            "lsk=" + uslVolKart.kart.getLsk() + ", usl=" + uslVolKart.usl.getId() +
                                                            ", diffDist=" + diffDist);
                                                }
                                                volDist = volDist.add(diffDist);
                                            }
                                            log.info("экономия: lsk={}, kpr={}, собств.объем={}, к распр={}",
                                            uslVolKartGrp.kart.getLsk(), uslVolKartGrp.kpr, uslVolKart.vol, volDist);

                                            // добавить объем для сохранения в C_CHARGE_PREP
                                            if (uslVolKart.isMeter) {
                                                addDistVol(mapDistMeterVol, mapDistVol, uslVolKart, limit, volDist);
                                            } else {
                                                addDistVol(mapDistNormVol, mapDistVol, uslVolKart, limit, volDist);
                                            }
                                        }
                                    }

                                    // СОХРАНИТЬ объем экономии и инфу по ОДН. по нормативу и счетчику в C_CHARGE_PREP
                                    // по счетчику
                                    mapDistMeterVol.entrySet().forEach(t -> addChargePrep(usl, t, true));
                                    // по нормативу
                                    mapDistNormVol.entrySet().forEach(t -> addChargePrep(usl, t, false));
                                    // в целом, по C_CHARGE
                                    mapDistVol.entrySet().forEach(t -> addCharge(usl, t));
                                }
                            }
                        }
                    }
                } else if (Utl.in(usl.getFkCalcTp(), 14, 23)) {
                    /* Отопление Гкал, распределить по площади
                    ИЛИ
                    Распределение по прочей услуге, расчитываемой как расценка * vol_add, пропорционально площади
                      например, эл.энерг МОП в Кис., в ТСЖ, эл.эн.ОДН в Полыс.
                      здесь же распределяется услуга ОДН, которая не предполагает собой
                      начисление по основной услуге в лицевых счетах */
                    if (areaAmnt.compareTo(BigDecimal.ZERO) != 0) {
                        BigDecimal diff;
                        if (Utl.in(usl.getCd(), "эл.эн.ОДН", "эл.эн.МОП2", "эл.эн.учет УО ОДН") && !isWithoutLimit
                                && kub.compareTo(limitODN.amntVolODN) > 0 // ограничение распределения по законодательству
                                ) {
                            diff = limitODN.amntVolODN;
                        } else {
                            diff = kub.setScale(5, RoundingMode.HALF_UP);
                        }
                        BigDecimal diffDist = diff;
                        Iterator<UslVolKartGrp> iter = lstUslVolKartGrpBase.stream()
                                .filter(t -> t.usl.equals(vvod.getUsl()))
                                .iterator();

                        while (iter.hasNext()) {
                            UslVolKartGrp t = iter.next();
                            for (Nabor nabor : t.kart.getNabor()) {
                                if (nabor.getUsl().equals(usl)) {
                                    BigDecimal volDistKart;
                                    if (iter.hasNext()) {
                                        volDistKart = diff.multiply(t.area.divide(areaAmnt, 5, RoundingMode.HALF_UP))
                                                .setScale(5, RoundingMode.HALF_UP);
                                    } else {
                                        // остаток объема, в т.ч. округление
                                        volDistKart = diffDist;
                                    }
                                    if (usl.getFkCalcTp().equals(14)) {
                                        nabor.setVol(volDistKart);
                                    } else {
                                        nabor.setVolAdd(volDistKart);
                                    }
                                    diffDist = diffDist.subtract(volDistKart);
                                    log.info("распределено: lsk={}, usl={}, kub={}, vol={}, area={}, areaAmnt={}",
                                            t.kart.getLsk(), usl.getId(), kub, volDistKart, t.area, areaAmnt);
                                }
                            }

                        }
                    }
                }
            }

            //log.trace("Итоговые объемы по вводу:");
            //log.trace("oplAdd={}, oplAr={}", vvod.getOplAdd(), vvod.getOplAr());

            // РАСПРЕДЕЛИТЬ объемы в домах с ОДПУ


            // РАСПРЕДЕЛИТЬ объемы в домах без ОДПУ
        }
    }

    /**
     * Добавить информацию распределения объема
     *
     * @param usl          - услуга
     * @param entry        - информационная строка
     * @param isExistMeter - наличие счетчика
     */
    private void addChargePrep(Usl usl, Map.Entry<Kart, Pair<BigDecimal, BigDecimal>> entry,
                               boolean isExistMeter) {
        Kart kart = entry.getKey();
        Pair<BigDecimal, BigDecimal> mapVal = entry.getValue();
        BigDecimal vol = mapVal.getValue0().setScale(5, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal("-1"));

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
     * @param usl   - услуга
     * @param entry - информационная строка
     */
    private void addCharge(Usl usl, Map.Entry<Kart, Pair<BigDecimal, BigDecimal>> entry) {
        Kart kart = entry.getKey();
        Pair<BigDecimal, BigDecimal> mapVal = entry.getValue();
        BigDecimal vol = mapVal.getValue0().setScale(5, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal("-1"));

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
     * @param tp            - тип услуги
     * @param distTp        - тип распределения
     * @param isUseSch      - учитывать счетчики?
     * @param uslVolKartGrp - запись объема сгруппированная   @return учитывать ли объем?
     */
    private boolean getIsCountOpl(int tp, Integer distTp, Boolean isUseSch, UslVolKartGrp uslVolKartGrp) {
        boolean isCountVol = true;
        if (tp == 3) {
            // прочая услуга, расчитываемая как расценка * vol_add, пропорционально площади
            isCountVol = true;
        } else if (distTp.equals(3)) {
            // тип распр.=3 то либо арендатор, либо должен кто-то быть прописан
            isCountVol = !uslVolKartGrp.kart.isResidental() || uslVolKartGrp.isExistPersCurrPeriod;
        } else if (!distTp.equals(3) && !isUseSch) {
            // тип распр.<>3 контролировать и нет наличия счетчиков в текущем периоде
            isCountVol = uslVolKartGrp.isExistMeterCurrPeriod;
        }
        return isCountVol;
    }


    /**
     * Очистка распределенных объемов
     *
     * @param vvod - ввод
     */
    private void clearODN(Vvod vvod) {
        // почистить нормативы (ограничения)
        log.trace("Очистка информации usl={}", vvod.getUsl().getId());
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
     * @param tp      - тип услуги
     * @param cntKpr  - кол во прожив. по вводу
     * @param area    - площадь по вводу
     */
    private LimitODN calcLimit(Ko houseKo, int tp, BigDecimal cntKpr, BigDecimal area) throws
            WrongParam, WrongGetMethod {
        LimitODN limitODN = new LimitODN();
        if (tp == 0) {
            // х.в. г.в.
            //расчитать лимит распределения
            //если кол-во прожив. > 0
            if (cntKpr.compareTo(BigDecimal.ZERO) != 0) {
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
            // норматив по эл.энерг. ОДН в доме без лифта
            final BigDecimal ODN_EL_NORM = new BigDecimal("2.7");
            // норматив по эл.энерг. ОДН в доме с лифтом
            final BigDecimal ODN_EL_NORM_WITH_LIFT = new BigDecimal("4.1");
            // эл.эн.
            // площадь общ.имущ., норматив, объем на площадь
            limitODN.areaProp = Utl.nvl(objParMng.getBd(houseKo, "area_general_property"), BigDecimal.ZERO);
            BigDecimal existLift = Utl.nvl(objParMng.getBd(houseKo, "exist_lift"), BigDecimal.ZERO);

            if (existLift.compareTo(BigDecimal.ZERO) == 0) {
                // дом без лифта
                limitODN.odnNorm = ODN_EL_NORM;
            } else {
                // дом с лифтом
                limitODN.odnNorm = ODN_EL_NORM_WITH_LIFT;
            }
            // общий объем ОДН
            limitODN.amntVolODN = limitODN.areaProp.multiply(limitODN.odnNorm);
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
        // общий объем ОДН (используется для ОДН электроэнергии)
        public BigDecimal amntVolODN;
    }

    class kartVol implements DistributableBigDecimal {
        // лиц.счет
        Kart kart;
        // объем
        BigDecimal vol;

        @Override
        public BigDecimal getBdForDist() {
            return null;
        }

        @Override
        public void setBdForDist(BigDecimal bd) {

        }
    }

}