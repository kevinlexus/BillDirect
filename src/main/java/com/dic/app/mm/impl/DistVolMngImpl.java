package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistVolMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.VvodDAO;
import com.dic.bill.dto.UslVolKart;
import com.dic.bill.dto.UslVolKartGrp;
import com.dic.bill.mm.ObjParMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDist;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Iterator;
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
public class DistVolMngImpl implements DistVolMng, CommonConstants {

    @PersistenceContext
    private EntityManager em;
    //@Autowired
    //private ProcessMng processMng;
    @Autowired
    private VvodDAO vvodDAO;
    @Autowired
    private ProcessMng processMng;
    @Autowired
    private ObjParMng objParMng;
    @Autowired
    private ConfigApp config;
    @Autowired
    private ApplicationContext ctx;

    /**
     * Вызов из Web контроллера
     *
     * @param reqConf - запрос
     * @param vvodId  - Id ввода
     */
    @Transactional(
            propagation = Propagation.REQUIRES_NEW, // новая транзакция
            rollbackFor = Exception.class)
    @Override
    public void distVolByVvodTrans(RequestConfigDirect reqConf, Long vvodId)
            throws ErrorWhileDist {
        distVolByVvod(reqConf, vvodId);
    }

    /**
    /**
     * Распределить объемы по вводу
     *
     * @param reqConf - параметры запроса
     * @param vvodId  - ввод
     */
    private void distVolByVvod(RequestConfigDirect reqConf, Long vvodId)
            throws ErrorWhileDist {
        if (!config.getLock().aquireLockId(reqConf.getRqn(), 1, vvodId, 60)) {
            throw new ErrorWhileDist("ОШИБКА БЛОКИРОВКИ vvodId=" + vvodId);
        }
        try {
            Vvod vvod = em.find(Vvod.class, vvodId);
            log.info("");
            log.info("Распределение объемов по vvodId={}, usl={}", vvodId, vvod.getUsl().getId());
            // тип распределения
            final Integer distTp = Utl.nvl(vvod.getDistTp(), 0);
            // использовать счетчики при распределении?
            final Boolean isUseSch = Utl.nvl(vvod.getIsUseSch(), false);

            // объем для распределения
            BigDecimal kub = Utl.nvl(vvod.getKub(), BigDecimal.ZERO).setScale(5, BigDecimal.ROUND_HALF_UP);
            final Usl usl = vvod.getUsl();
            // неограничивать лимитом потребления ОДН?
            final boolean isWithoutLimit = Utl.nvl(vvod.getIsWithoutLimit(), false);

            // тип услуги
            int tpTmp = -1;

            // вид услуги ограничения ОДН
            if (usl.getFkCalcTp() == null) {
                // не заполнен вариант расчета - возможно услуга для распределения
                // объема - информационно - не распределять вообще
                tpTmp = -1;
            } else if (Utl.in(usl.getFkCalcTp(), 3, 17, 4, 18, 31, 38, 40)) {
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
                if (kub.compareTo(BigDecimal.ZERO) != 0) {
                    // конфиг для расчета по вводу
                    RequestConfigDirect reqConf2;
                    try {
                        reqConf2 = (RequestConfigDirect) reqConf.clone();
                    } catch (CloneNotSupportedException e) {
                        log.error(Utl.getStackTraceString(e));
                        throw new ErrorWhileDist("ОШИБКА! RequestConfig не может быть склонирован!");
                    }
                    reqConf2.setUk(null);
                    reqConf2.setHouse(null);
                    reqConf2.setKo(null);
                    reqConf2.setVvod(vvod);
                    reqConf2.setTp(3); // начисление для распределения по вводу
                    reqConf2.prepareId();
                    reqConf2.prepareChrgCountAmount();

                    // СБОР ИНФОРМАЦИИ, для расчета ОДН, подсчета итогов
                    // кол-во лиц.счетов, объемы, кол-во прожив.
                    // собрать информацию об объемах по лиц.счетам принадлежащим вводу

                    ProcessMng processMng = ctx.getBean(ProcessMng.class);
                    processMng.processAll(reqConf2);

                    // объемы по лиц.счетам (базовый фильтр по услуге)
                    final List<UslVolKart> lstUslVolKart =
                            reqConf2.getChrgCountAmount().getLstUslVolKart().stream()
                                    .filter(t -> t.getUsl().equals(usl)).collect(Collectors.toList());
                    final List<UslVolKartGrp> lstUslVolKartGrpBase =
                            reqConf2.getChrgCountAmount().getLstUslVolKartGrp().stream()
                                    .filter(t -> t.getUsl().equals(usl)).collect(Collectors.toList());

                    // ПОЛУЧИТЬ итоговые объемы по вводу
                    List<UslVolKart> lstUslVolKartStat;
                    if (Utl.in(tp, 0, 2)) {
                        // х.в., г.в., эл.эн., эл.эн.ОДН
                        lstUslVolKartStat =
                                lstUslVolKart.stream()
                                        .filter(t ->
                                                t.getKart().getNabor().stream()
                                                        .anyMatch((d ->
                                                                distTp!=8 && d.getUsl().equals(t.getUsl().getUslChild()) || // должна быть дочерняя услуга в карточках! например 011->124 для Кис! ред.03.10.2019
                                                                        distTp==8 && d.getUsl().equals(t.getUsl()) // распределение только для информации
                                                        )) // где есть наборы по дочерним усл.
                                                        && getIsCountOpl(tp, distTp, isUseSch, t)
                                        ).collect(Collectors.toList());
                        if (lstUslVolKartStat.size() == 0) {
                            log.warn("ВНИМАНИЕ! По вводу vvodId={} не были получены характеристики " +
                                    "(кол-во прож, м2 и т.п.)! Возможно отсутствует дочерние услуги " +
                                    "в карточках лиц.счетов, например для 011 услуги должна быть 124.", vvodId);
                        }
                        for (UslVolKart uslVolKartGrp : lstUslVolKartStat) {
                            // сохранить объемы по вводу для статистики
                            if (uslVolKartGrp.isResidental()) {
                                // по жилым помещениям
                                if (uslVolKartGrp.isMeter()) {
                                    // по счетчикам
                                    // объем
                                    vvod.setKubSch(vvod.getKubSch().add(uslVolKartGrp.getVol()));
                                    // кол-во лицевых
                                    vvod.setSchCnt(vvod.getSchCnt().add(new BigDecimal("1")));
                                    if (!uslVolKartGrp.isEmpty() || uslVolKartGrp.getVol().compareTo(BigDecimal.ZERO) > 0) {
                                        // кол-во проживающих
                                        vvod.setSchKpr(vvod.getSchKpr().add(uslVolKartGrp.getKprNorm()));
                                    }
                                } else {
                                    // по нормативам
                                    // объем
                                    vvod.setKubNorm(vvod.getKubNorm().add(uslVolKartGrp.getVol()));
                                    // кол-во лицевых
                                    vvod.setCntLsk(vvod.getCntLsk().add(new BigDecimal("1")));
                                    if (!uslVolKartGrp.isResidental() || uslVolKartGrp.getVol().compareTo(BigDecimal.ZERO) > 0) {
                                        // кол-во проживающих
                                        vvod.setKpr(vvod.getKpr().add(uslVolKartGrp.getKprNorm()));
                                    }
                                }

                            } else {
                                // по нежилым помещениям
                                // площадь
                                vvod.setOplAr(vvod.getOplAr().add(uslVolKartGrp.getArea()));
                                // объем
                                vvod.setKubAr(vvod.getKubAr().add(uslVolKartGrp.getVol()));
                                if (uslVolKartGrp.isMeter()) {
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
                            vvod.setOplAdd(Utl.nvl(vvod.getOplAdd(), BigDecimal.ZERO).add(uslVolKartGrp.getArea()));
                        }
                    } else if (tp == 3) {
                        // Отопление Гкал
                        for (UslVolKart t : lstUslVolKart) {
                            //log.trace("usl={}, cnt={}, empt={}, resid={}, t.vol={}, t.area={}",
                            //        t.usl.getId(), t.isMeter, t.isEmpty, t.isResidental, t.vol, t.area);
                            // кол-во лицевых
                            vvod.setSchCnt(vvod.getSchCnt().add(new BigDecimal("1")));
                            // кол-во проживающих
                            vvod.setSchKpr(vvod.getSchKpr().add(t.getKprNorm()));
                            if (!t.isResidental()) {
                                // сохранить объемы по вводу для статистики
                                // площадь по нежилым помещениям
                                vvod.setOplAr(Utl.nvl(vvod.getOplAr(), BigDecimal.ZERO).add(t.getArea()));
                            }
                            // площадь по вводу
                            vvod.setOplAdd(Utl.nvl(vvod.getOplAdd(), BigDecimal.ZERO).add(t.getArea()));
                        }
                        // округлить площади по вводу
                        vvod.setOplAr(vvod.getOplAr().setScale(5, RoundingMode.HALF_UP));
                        vvod.setOplAdd(vvod.getOplAdd().setScale(5, RoundingMode.HALF_UP));
                    }


                    Amnt amnt = new Amnt();
                    // Итоги
                    // объем
                    amnt.volAmnt = vvod.getKubNorm().add(vvod.getKubSch()).add(vvod.getKubAr())
                            .setScale(5, BigDecimal.ROUND_HALF_UP);
                    amnt.volNormAmnt = vvod.getKubNorm()
                            .setScale(5, BigDecimal.ROUND_HALF_UP);
                    // объем кроме арендаторов
                    amnt.volAmntResident = vvod.getKubNorm().add(vvod.getKubSch())
                            .setScale(5, BigDecimal.ROUND_HALF_UP);
                    // кол-во проживающих
                    amnt.kprAmnt = vvod.getKpr().add(vvod.getSchKpr())
                            .setScale(5, BigDecimal.ROUND_HALF_UP);
                    // площадь по вводу, варьируется от услуги
                    amnt.areaAmnt = vvod.getOplAdd()
                            .setScale(5, BigDecimal.ROUND_HALF_UP);
                    // кол-во лиц.счетов по счетчикам
                    amnt.cntSchAmnt = vvod.getSchCnt();
                    // кол-во лиц.счетов по нормативам
                    amnt.cntNormAmnt = vvod.getCntLsk();
                    amnt.distFactUpNorm = BigDecimal.ZERO;

                    log.info("*** Ввод vvodId={}, услуга usl={}, площадь={}, кол-во лиц сч.={}, кол-во лиц норм.={}, кол-во прож.={}, " +
                                    "объем={}, объем сч={}, объем норм.={}" +
                                    " объем за искл.аренд.={},  введено={}, вкл.счетчики={}",
                            vvod.getId(), vvod.getUsl().getId(), amnt.areaAmnt, amnt.cntSchAmnt, amnt.cntNormAmnt,
                            amnt.kprAmnt, amnt.volAmnt, amnt.volSchAmnt, amnt.volNormAmnt, amnt.volAmntResident, kub,
                            isUseSch);

                    // ОГРАНИЧЕНИЕ распределения по законодательству
                    final LimitODN limitODN = calcLimit(vvod.getHouse().getKo(), tp, amnt.kprAmnt, amnt.areaAmnt);

                    // РАСПРЕДЕЛЕНИЕ
                    if (!Utl.in(distTp, 4, 5)) {
                        // с ОДПУ
                        if (kub.compareTo(BigDecimal.ZERO) != 0) {
                            if (Utl.in(usl.getFkCalcTp(), 3, 17, 4, 18, 31, 38, 40)) {
                                if (Utl.in(distTp, 1, 3, 8)) {
                                    BigDecimal volAmntWithODN = (amnt.volAmnt.add(limitODN.amntVolODN))
                                            .setScale(3, BigDecimal.ROUND_HALF_UP);
                                    if (!isWithoutLimit && volAmntWithODN.compareTo(BigDecimal.ZERO) > 0) {
                                        log.info("*** органичение ОДН по вводу ={}", limitODN.amntVolODN);
                                        if (kub.compareTo(volAmntWithODN) > 0) {
                                            // установить предельно допустимый объем по дому
                                            amnt.distFactUpNorm = kub.subtract(volAmntWithODN);
                                            kub = volAmntWithODN;
                                            log.info("*** установлен новый объем для распределения по вводу ={}", volAmntWithODN);
                                        }
                                    }

                                    BigDecimal diff = kub.subtract(amnt.volAmnt);
                                    BigDecimal diffDist = diff.abs();
                                    if (diff.compareTo(BigDecimal.ZERO) != 0 && amnt.areaAmnt.compareTo(BigDecimal.ZERO) != 0) {
                                        // выборка для распределения
                                        // есть небаланс
                                        if (diff.compareTo(BigDecimal.ZERO) > 0) {
                                            // ПЕРЕРАСХОД
                                            log.info("*** перерасход={}", diff);
                                            // доначисление пропорционально площади (в т.ч.арендаторы), если небаланс > 0
                                            List<UslVolKartGrp> lstUslVolKartGrp = lstUslVolKartGrpBase.stream()
                                                    .filter(t -> t.getUsl().equals(usl) &&
                                                            t.getKart().getNabor().stream()
                                                                    .anyMatch((d ->
                                                                            distTp!=8 && d.getUsl().equals(t.getUsl().getUslChild()) ||
                                                                                    distTp==8 && d.getUsl().equals(t.getUsl()) // распределение только для информации
                                                                    )) // где есть наборы по дочерним усл.
                                                            && getIsCountOpl(tp, distTp, isUseSch, t)).collect(Collectors.toList());
                                            Iterator<UslVolKartGrp> iter = lstUslVolKartGrp.iterator();
                                            while (iter.hasNext()) {
                                                UslVolKartGrp uslVolKartGrp = iter.next();
                                                // по дочерним услугам
                                                // рассчитать долю объема
                                                BigDecimal proc = uslVolKartGrp.getArea().divide(amnt.areaAmnt, 20, BigDecimal.ROUND_HALF_UP);
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
                                                    limitTmp = limitODN.limitByArea.multiply(uslVolKartGrp.getArea());
                                                } else if (tp == 2) {
                                                    // эл.эн.
                                                    limitTmp = limitODN.amntVolODN // взято из P_VVOD строка 591
                                                            .multiply(uslVolKartGrp.getArea()).divide(amnt.areaAmnt, 20, BigDecimal.ROUND_HALF_UP);
                                                }
                                                BigDecimal limit = limitTmp;
                                                // uslVolKartGrp.kart - был из другой сессии, поэтому, найти опять
                                                Kart kart = em.find(Kart.class, uslVolKartGrp.getKart().getLsk());
                                                kart.getNabor().stream()
                                                        .filter(n -> n.getUsl().equals(usl.getUslChild()))
                                                        .findFirst().ifPresent(n -> {
                                                    log.info("Перерасход lsk={}, usl={}, vol={}",
                                                            n.getKart().getLsk(), n.getUsl().getId(), volDist);
                                                    n.setLimit(limit);
                                                    //log.info("$$$$$3, nabor.id={}, nabor.volAdd={}", n.getId(), volDist);
                                                    //n.setVolAdd(volDist);
                                                });
                                                // добавить инфу по ОДН.
                                                if (volDist.compareTo(BigDecimal.ZERO) != 0) {
                                                    Charge charge = new Charge();
                                                    kart.getCharge().add(charge);
                                                    charge.setKart(kart);
                                                    charge.setUsl(uslVolKartGrp.getUsl().getUslChild());
                                                    charge.setTestOpl(volDist);
                                                    charge.setType(5);
                                                    // добавить итоговые объемы доначисления
                                                    addAmnt(amnt, volDist, uslVolKartGrp);
                                                }
                                            }
                                        } else {
                                            // ЭКОНОМИЯ - рассчитывается пропорционально кол-во проживающих, кроме Нежилых
                                            // считается без ОКРУГЛЕНИЯ, так как экономия может быть срезана текущим объемом!
                                            if (amnt.kprAmnt.compareTo(BigDecimal.ZERO) != 0) {
                                                BigDecimal diffPerPers = diffDist.divide(amnt.kprAmnt, 20, BigDecimal.ROUND_HALF_UP);
                                                log.info("*** экономия={}, на 1 прожив={}", diffDist, diffPerPers);
                                                // лиц.счет, объем, лимит
                                                // по счетчику
                                                Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistMeterVol = new HashMap<>();
                                                // по нормативу
                                                Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistNormVol = new HashMap<>();
                                                // общий
                                                Map<Kart, Pair<BigDecimal, BigDecimal>> mapDistVol = new HashMap<>();
                                                boolean isRestricted = false;
                                                List<UslVolKartGrp> lstUslVolKartGrp = lstUslVolKartGrpBase.stream()
                                                        .filter(t -> t.isResidental() && // здесь только жилые помещения
                                                                t.getKart().getNabor().stream()
                                                                        .anyMatch((d ->
                                                                                distTp!=8 && d.getUsl().equals(t.getUsl().getUslChild()) ||
                                                                                        distTp==8 && d.getUsl().equals(t.getUsl()) // распределение только для информации
                                                                        )) // где есть наборы по дочерним усл.

                                                                && getIsCountOpl(tp, distTp, isUseSch, t)).collect(Collectors.toList());
                                                Iterator<UslVolKartGrp> iter = lstUslVolKartGrp.iterator();
                                                while (iter.hasNext()) {
                                                    UslVolKartGrp uslVolKartGrp = iter.next();

                                                    // лимит (информационно)
                                                    BigDecimal limitTmp = null;
                                                    if (Utl.in(tp, 0)) {
                                                        // х.в. г.в.
                                                        limitTmp = limitODN.limitByArea.multiply(uslVolKartGrp.getArea());
                                                    } else if (tp == 2) {
                                                        // эл.эн.
                                                        limitTmp = limitODN.amntVolODN // взято из P_VVOD строка 591
                                                                .multiply(uslVolKartGrp.getArea())
                                                                .divide(amnt.areaAmnt, 3, BigDecimal.ROUND_HALF_UP);
                                                    }
                                                    BigDecimal limit = limitTmp;
                                                    // установить лимит
                                                    Kart kart = em.find(Kart.class, uslVolKartGrp.getKart().getLsk());
                                                    kart.getNabor().stream()
                                                            .filter(d -> d.getUsl().equals(uslVolKartGrp.getUsl().getUslChild()))
                                                            .findFirst().ifPresent(d -> d.setLimit(limit));
/*
                                                for (UslVolKart e : lstUslVolKart) {
                                                    log.info("$$$$$$ e.kart.getLsk()={}, e.isEmpty={}, e.kprNorm={} ",
                                                            e.kart.getLsk(), e.isEmpty, e.kprNorm);
                                                }
*/
                                                    // распределить экономию в доле на кол-во проживающих
                                                    List<UslVolKart> lstUslVolKart2 = lstUslVolKart.stream()
                                                            .filter(t -> uslVolKartGrp.getKart().equals(t.getKart()) &&
                                                                    t.getKprNorm().compareTo(BigDecimal.ZERO) > 0)
                                                            .collect(Collectors.toList());
                                                    Iterator<UslVolKart> iter2 = lstUslVolKart2.iterator();
                                                    while (iter2.hasNext()) {
                                                        UslVolKart uslVolKart = iter2.next();
                                                        BigDecimal volDist = uslVolKart.getKprNorm().multiply(diffPerPers)
                                                                .setScale(5, BigDecimal.ROUND_HALF_UP);
                                                        // ограничить объем экономии текущим общим объемом норматив+счетчик
                                                        if (volDist.compareTo(uslVolKart.getVol()) > 0) {
                                                            log.info("ОГРАНИЧЕНИЕ экономии: lsk={}, vol={}",
                                                                    uslVolKart.getKart().getLsk(),
                                                                    volDist.subtract(uslVolKart.getVol()));
                                                            isRestricted = true;
                                                            volDist = uslVolKart.getVol();
                                                        }

                                                        diffDist = diffDist.subtract(volDist);
                                                        if (!iter.hasNext() && !iter2.hasNext() && !isRestricted) {
                                                            // остаток на последнюю строку, если не было ограничений экономии
                                                            if (diffDist.abs().compareTo(new BigDecimal("0.05")) > 0) {
                                                                throw new ErrorWhileDist("ОШИБКА! Некорректный объем округления, " +
                                                                        "lsk=" + uslVolKart.getKart().getLsk() + ", usl="
                                                                        + uslVolKart.getUsl().getId() +
                                                                        ", diffDist=" + diffDist);
                                                            }
                                                            volDist = volDist.add(diffDist);
                                                        }
                                                        log.info("экономия: lsk={}, kpr={}, собств.объем={}, к распр={}",
                                                                uslVolKartGrp.getKart().getLsk(),
                                                                uslVolKartGrp.getKprNorm(), uslVolKart.getVol(), volDist);

                                                        // добавить объем для сохранения в C_CHARGE_PREP
                                                        if (uslVolKart.isMeter()) {
                                                            addDistVol(mapDistMeterVol, mapDistVol, uslVolKart, limit, volDist);
                                                        } else {
                                                            addDistVol(mapDistNormVol, mapDistVol, uslVolKart, limit, volDist);
                                                        }
                                                        // добавить итоговые объемы экономии
                                                        addAmnt(amnt, volDist.negate(), uslVolKartGrp);
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
                                if (amnt.areaAmnt.compareTo(BigDecimal.ZERO) != 0) {
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
                                            .filter(t -> t.getUsl().equals(vvod.getUsl()))
                                            .iterator();

                                    while (iter.hasNext()) {
                                        UslVolKartGrp t = iter.next();
                                        Kart kart = em.find(Kart.class, t.getKart().getLsk());
                                        for (Nabor nabor : kart.getNabor()) {
                                            if (nabor.getUsl().equals(usl)) {
                                                BigDecimal volDistKart;
                                                //if (iter.hasNext()) {
                                                volDistKart = diff.multiply(t.getArea().divide(amnt.areaAmnt, 20,
                                                        RoundingMode.HALF_UP))
                                                        .setScale(5, RoundingMode.HALF_UP);
                                            /*} else { убрал - округление не даёт выйти на результаты Кис. ред. 05.03.19
                                                // остаток объема, в т.ч. округление
                                                volDistKart = diffDist;
                                                log.trace("Объем, в т.ч. округление упало на lsk={}",
                                                        nabor.getKart().getLsk());
                                            }*/
                                                if (usl.getFkCalcTp().equals(14)) {
                                                    //log.info("$$$$$4, nabor.id={}, nabor.vol={}",
                                                            //nabor.getId(), volDistKart);
                                                    nabor.setVol(volDistKart);
                                                } else {
                                                    //log.info("$$$$$5, nabor.id={}, nabor.volAdd={}",
                                                            //nabor.getId(), volDistKart);
                                                    nabor.setVolAdd(volDistKart);
                                                }
                                                // добавить итоговые объемы доначисления
                                                addAmnt(amnt, volDistKart, t);

                                                diffDist = diffDist.subtract(volDistKart);
                                                log.info("распределено: lsk={}, usl={}, kub={}, vol={}, area={}, areaAmnt={}",
                                                        t.getKart().getLsk(), usl.getId(),
                                                        kub, volDistKart, t.getArea(), amnt.areaAmnt);
                                            }
                                        }

                                    }
                                }
                            }
                        }
                    } else {
                        // без ОДПУ
                        if (tp == 0 && amnt.kprAmnt.compareTo(BigDecimal.ZERO) != 0 || tp == 2) {
                            // если кол-во проживающих <>0, (для х.в. и г.в.) или по эл.эн.
                            List<UslVolKartGrp> lstUslVolKartGrp = lstUslVolKartGrpBase.stream()
                                    .filter(t -> t.isResidental() && // здесь только жилые помещения
                                            t.getKart().getNabor().stream()
                                                    .anyMatch((d ->
                                                            distTp!=8 && d.getUsl().equals(t.getUsl().getUslChild()) ||
                                                                    distTp==8 && d.getUsl().equals(t.getUsl()) // распределение только для информации
                                                    )) // где есть наборы по дочерним усл.
                                            && getIsCountOpl(tp, distTp, isUseSch, t)).collect(Collectors.toList());

                            for (UslVolKartGrp uslVolKartGrp : lstUslVolKartGrp) {
                                // лимит (информационно)
                                BigDecimal limitTmp = null;
                                if (Utl.in(tp, 0)) {
                                    // х.в., г.в.
                                    limitTmp = limitODN.limitByArea.multiply(uslVolKartGrp.getArea());
                                } else if (tp == 2) {
                                    // эл.эн.
                                    limitTmp = limitODN.amntVolODN // взято из P_VVOD строка 591
                                            .multiply(uslVolKartGrp.getArea()).divide(amnt.areaAmnt, 3, BigDecimal.ROUND_HALF_UP);
                                }
                                BigDecimal limit = limitTmp;
                                // установить лимит
                                Kart kart = em.find(Kart.class, uslVolKartGrp.getKart().getLsk());
                                kart.getNabor().stream()
                                        .filter(d -> d.getUsl().equals(uslVolKartGrp.getUsl().getUslChild()))
                                        .findFirst().ifPresent(d -> d.setLimit(limit));
                                // распределить норматив ОДН
                                BigDecimal volDistTmp = BigDecimal.ZERO;
                                if (tp == 0) {
                                    // х.в., г.в.
                                    volDistTmp = uslVolKartGrp.getArea().multiply(limitODN.limitByArea)
                                            .setScale(5, BigDecimal.ROUND_HALF_UP);
                                } else if (tp == 2) {
                                    // эл.эн.
                                    volDistTmp = uslVolKartGrp.getArea().multiply(limitODN.odnNorm)
                                            .divide(amnt.areaAmnt, 5, BigDecimal.ROUND_HALF_UP);
                                }
                                BigDecimal volDist = volDistTmp;

                                kart.getNabor().stream()
                                        .filter(n -> n.getUsl().equals(usl.getUslChild()))
                                        .findFirst().ifPresent(n -> {
                                    log.info("Норматив ОДН lsk={}, usl={}, vol={}",
                                            n.getKart().getLsk(), n.getUsl().getId(), volDist);
                                    //log.info("$$$$$6, nabor.id={}, nabor.volAdd={}", n.getId(), volDist);
                                    n.setLimit(limit);
                                    n.setVolAdd(volDist);
                                });
                                // добавить инфу по ОДН.
                                if (volDist.compareTo(BigDecimal.ZERO) != 0) {
                                    Charge charge = new Charge();
                                    kart.getCharge().add(charge);
                                    charge.setKart(kart);
                                    charge.setUsl(usl.getUslChild());
                                    charge.setTestOpl(volDist);
                                    charge.setType(5);
                                    // добавить итоговые объемы доначисления
                                    addAmnt(amnt, volDist, uslVolKartGrp);
                                }
                            }
                        }
                    }

                    // ИТОГОВЫЕ показатели по вводу, округлить
                    vvod.setKpr(vvod.getKpr().setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setSchKpr(vvod.getSchKpr().setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setNrm(limitODN.odnNorm);
                    vvod.setKubNrmFact(amnt.distNormFact.setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setKubSchFact(amnt.distSchFact.setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setKubFact(amnt.distFact.setScale(5, BigDecimal.ROUND_HALF_UP));

                    // ограничить Итого распределено по Введенному объему
                    BigDecimal tmpKubDist = amnt.volAmnt.add(
                            amnt.distFact.setScale(5, BigDecimal.ROUND_HALF_UP));
                    if (tmpKubDist.compareTo(kub) > 0) {
                        tmpKubDist = kub;
                    }
                    vvod.setKubDist(tmpKubDist);
                    vvod.setKubSch(vvod.getKubSch().setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setKubNorm(vvod.getKubNorm().setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setKubFactUpNorm(amnt.distFactUpNorm.setScale(5, BigDecimal.ROUND_HALF_UP));
                    vvod.setOplAdd(vvod.getOplAdd().setScale(5, BigDecimal.ROUND_HALF_UP));

                    log.info("Итого по вводу распределено: " +
                                    "vvod.getNrm()={}, vvod.getKubNrmFact()={}, vvod.getKubSchFact()={}, " +
                                    "vvod.getKubFact()={}, vvod.getKubDist()={}",
                            vvod.getNrm(), vvod.getKubNrmFact(), vvod.getKubSchFact(),
                            vvod.getKubFact(), vvod.getKubDist());
                }
            }
        } catch (WrongParam | WrongGetMethod | ErrorWhileGen e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileDist("ОШИБКА ПРИ РАСПРЕДЕЛЕНИИ ОБЪЕМОВ!");
        } finally {
            // разблокировать помещение
            config.getLock().unlockId(reqConf.getRqn(), 1, vvodId);
            log.info("******* vvodId={} разблокирован после расчета", vvodId);
        }
    }

    /**
     * Добавить итоговые, распределенные объемы
     *
     * @param amnt          - объект с итоговыми значениями
     * @param volDist       - объем распределения
     * @param uslVolKartGrp - строка объема
     */
    private void addAmnt(Amnt amnt, BigDecimal volDist, UslVolKartGrp uslVolKartGrp) {
        Usl usl = uslVolKartGrp.getUsl();
        Kart kart = uslVolKartGrp.getKart();
        if (Utl.in(usl.getFkCalcTp(), 3, 17, 38) && kart.isExistColdWaterMeter()) {
            // объем по счетчику
            amnt.distSchFact = amnt.distSchFact.add(volDist);
        } else if (Utl.in(usl.getFkCalcTp(), 4, 18, 40) && kart.isExistHotWaterMeter()) {
            // объем по счетчику
            amnt.distSchFact = amnt.distSchFact.add(volDist);
        } else if (Utl.in(usl.getFkCalcTp(), 31) && kart.isExistElMeter()) {
            // объем по счетчику
            amnt.distSchFact = amnt.distSchFact.add(volDist);
        } else {
            // объем по нормативу
            amnt.distNormFact = amnt.distNormFact.add(volDist);
        }
        // общий объем
        amnt.distFact = amnt.distFact.add(volDist);
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
        // получить Kart, так как entry.getKey() - из другой сессии
        Kart kart = em.find(Kart.class, entry.getKey().getLsk());
        Pair<BigDecimal, BigDecimal> mapVal = entry.getValue();
        BigDecimal vol = mapVal.getValue0().setScale(5, BigDecimal.ROUND_HALF_UP).negate();

        ChargePrep chargePrep = new ChargePrep();
        kart.getChargePrep().add(chargePrep);
        chargePrep.setKart(kart);
        chargePrep.setUsl(usl);
        chargePrep.setVol(vol);
        chargePrep.setTp(4);
        chargePrep.setIsExistMeter(isExistMeter);
    }

    /**
     * Добавить информацию распределения объема
     *
     * @param usl   - услуга
     * @param entry - информационная строка
     */
    private void addCharge(Usl usl, Map.Entry<Kart, Pair<BigDecimal, BigDecimal>> entry) {
        // получить Kart, так как entry.getKey() - из другой сессии
        Kart kart = em.find(Kart.class, entry.getKey().getLsk());
        Pair<BigDecimal, BigDecimal> mapVal = entry.getValue();
        BigDecimal vol = mapVal.getValue0().setScale(5, BigDecimal.ROUND_HALF_UP).negate();

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
        Pair<BigDecimal, BigDecimal> mapVal = mapDistDetVol.get(t.getKart());
        if (mapVal == null) {
            mapDistDetVol.put(t.getKart(),
                    Pair.with(vol, limit));
        } else {
            mapVal = mapDistDetVol.get(t.getKart());
            mapDistDetVol.put(t.getKart(),
                    Pair.with(mapVal.getValue0().add(vol), limit));
        }

        mapVal = mapDistVol.get(t.getKart());
        if (mapVal == null) {
            mapDistVol.put(t.getKart(),
                    Pair.with(vol, limit));
        } else {
            mapVal = mapDistVol.get(t.getKart());
            mapDistVol.put(t.getKart(),
                    Pair.with(mapVal.getValue0().add(vol), limit));
        }

    }

    /**
     * Учитывать ли объем?
     *
     * @param tp            - тип услуги
     * @param distTp        - тип распределения
     * @param isUseSch      - учитывать счетчики?
     * @param uslVolKartGrp - запись объема сгруппированная
     * @return учитывать ли объем?
     */
    private boolean getIsCountOpl(int tp, Integer distTp, Boolean isUseSch, UslVolKartGrp uslVolKartGrp) {
        boolean isCountVol = true;
        if (tp == 3) {
            // прочая услуга, расчитываемая как расценка * vol_add, пропорционально площади
            isCountVol = true;
        } else if (distTp.equals(3)) {
            // тип распр.=3 то либо арендатор, либо должен кто-то быть прописан
            isCountVol = !uslVolKartGrp.getKart().isResidental() || uslVolKartGrp.isExistPersCurrPeriod();
        } else if (!distTp.equals(3) && !isUseSch) {
            // тип распр.<>3 контролировать и нет наличия счетчиков в текущем периоде
            isCountVol = !uslVolKartGrp.isExistMeterCurrPeriod();
        } else if (!distTp.equals(3) && isUseSch) {
            // note ред. 29.03.2019 - проверить
            // тип распр.<>3 контролировать и нет наличия счетчиков в текущем периоде
            isCountVol = true;
        }
        return isCountVol;
    }

    /**
     * Учитывать ли объем?
     *
     * @param tp         - тип услуги
     * @param distTp     - тип распределения
     * @param isUseSch   - учитывать счетчики?
     * @param uslVolKart - запись объема
     * @return учитывать ли объем?
     */
    private boolean getIsCountOpl(int tp, Integer distTp, Boolean isUseSch, UslVolKart uslVolKart) {
        boolean isCountVol = true;
        if (tp == 3) {
            // прочая услуга, расчитываемая как расценка * vol_add, пропорционально площади
            isCountVol = true;
        } else if (distTp.equals(3)) {
            // тип распр.=3 то либо арендатор, либо должен кто-то быть прописан
            isCountVol = !uslVolKart.getKart().isResidental() || uslVolKart.isMeter();
        } else if (!distTp.equals(3) && !isUseSch) {
            // тип распр.<>3 контролировать и нет наличия счетчиков в текущем периоде
            isCountVol = !uslVolKart.isMeter();
/*        } else if (!distTp.equals(3) && isUseSch) {
            // ред. 29.03.2019
            // тип распр.<>3 контролировать и нет наличия счетчиков в текущем периоде
            isCountVol = true;*/
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
                //log.info("$$$$$1, nabor.id={}, nabor.vol=null, nabor.volAdd=null, nabor.limit=null", nabor.getId());
                nabor.setVol(null);
                nabor.setVolAdd(null);
                nabor.setLimit(null);

            }

            for (Nabor nabor2 : nabor.getKart().getNabor()) {
                if (nabor2.getUsl().equals(vvod.getUsl().getUslChild())) {
                    // занулить по зависимым услугам
                    //log.info("$$$$$2, nabor.id={}, nabor.vol=null, nabor.volAdd=null, nabor.limit=null", nabor2.getId());
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
                // литров на 1 м2
                final BigDecimal oplLiter = oplLiter(oplMan.intValue());
/*
                limitODN.limitVol = oplLiter
                        .divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP);
                limitODN.odnNorm = limitODN.limitVol;
*/
                // норма ОДН в м3 на 1 м2
                limitODN.odnNorm = oplLiter;
                limitODN.limitByArea = oplLiter
                        .divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP);
                // общий допустимый объем ОДН
                limitODN.amntVolODN = limitODN.limitByArea.multiply(area).setScale(3, BigDecimal.ROUND_HALF_UP);
            }

        } else if (tp == 2) {
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
            // общий допустимый объем ОДН
            limitODN.amntVolODN = limitODN.areaProp.multiply(limitODN.odnNorm);
        }
        return limitODN;
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
        // допустимый лимит ОДН на 1 м2
        BigDecimal limitByArea = BigDecimal.ZERO;
        // площадь общего имущества
        BigDecimal areaProp = BigDecimal.ZERO;
        // общий объем ОДН (используется для ОДН электроэнергии)
        BigDecimal amntVolODN = BigDecimal.ZERO;
    }

    /**
     * DTO для хранения итоговых значений по вводу
     */
    class Amnt {
        // общий объем
        BigDecimal volAmnt = BigDecimal.ZERO;
        // объем по счетчикам
        BigDecimal volSchAmnt = BigDecimal.ZERO;
        // объем по нормативам
        BigDecimal volNormAmnt = BigDecimal.ZERO;
        // объем кроме арендаторов
        BigDecimal volAmntResident = BigDecimal.ZERO;
        // кол-во проживающих
        BigDecimal kprAmnt = BigDecimal.ZERO;
        // площадь по вводу, варьируется от услуги
        BigDecimal areaAmnt = BigDecimal.ZERO;
        // кол-во лиц.счетов по счетчикам
        BigDecimal cntSchAmnt = BigDecimal.ZERO;
        // кол-во лиц.счетов по нормативам
        BigDecimal cntNormAmnt = BigDecimal.ZERO;

        // распределено факт. на нормативы
        BigDecimal distNormFact = BigDecimal.ZERO;
        // распределено факт. на счетчики
        BigDecimal distSchFact = BigDecimal.ZERO;
        //
        BigDecimal distFact = BigDecimal.ZERO;

        BigDecimal distFactUpNorm = BigDecimal.ZERO;
    }

}