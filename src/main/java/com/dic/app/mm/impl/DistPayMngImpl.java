package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.app.mm.ProcessMng;
import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.NaborDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.SprProcPayDAO;
import com.dic.bill.dto.Amount;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис распределения оплаты
 *
 * @version 1.03
 */
@Slf4j
@Service
public class DistPayMngImpl implements DistPayMng {

    private final ProcessMng processMng;
    private final SaldoMng saldoMng;
    private final ConfigApp configApp;
    private final SprProcPayDAO sprProcPayDAO;
    private final SaldoUslDAO saldoUslDAO;
    private final ReferenceMng referenceMng;
    private final SprParamMng sprParamMng;

    @PersistenceContext
    private EntityManager em;

    public DistPayMngImpl(SaldoMng saldoMng, ConfigApp configApp,
                          SprProcPayDAO sprProcPayDAO, NaborDAO naborDAO, SaldoUslDAO saldoUslDAO,
                          ProcessMng processMng,
                          ReferenceMng referenceMng, SprParamMng sprParamMng) {
        this.saldoMng = saldoMng;
        this.configApp = configApp;
        this.sprProcPayDAO = sprProcPayDAO;
        this.saldoUslDAO = saldoUslDAO;
        this.processMng = processMng;
        this.referenceMng = referenceMng;
        this.sprParamMng = sprParamMng;
    }


    /**
     * Распределить платеж (запись в C_KWTP_MG)
     *
     * @param kwtpMgId - ID записи C_KWTP_MG
     * @param lsk      - лиц.счет
     * @param strSumma - сумма оплаты долга
     * @param strPenya - сумма оплаты пени
     * @param dopl     - период оплаты
     * @param nink     - № инкассации
     * @param nkom     - № компьютера
     * @param oper     - код операции
     * @param strDtek  - дата платежа
     * @param strDtInk - дата инкассации
     * @param isTest   - тестирование? (не будет вызвано начисление)
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
    public void distKwtpMg(int kwtpMgId,
                           String lsk,
                           String strSumma,
                           String strPenya,
                           String strDebt,
                           String dopl,
                           int nink,
                           String nkom,
                           String oper,
                           String strDtek,
                           String strDtInk,
                           boolean isTest) throws ErrorWhileDistPay {

        try {
            Amount amount = buildAmount(kwtpMgId,
                    !isTest,
                    lsk,
                    strSumma,
                    strPenya,
                    strDebt,
                    dopl,
                    nink,
                    nkom,
                    oper,
                    strDtek,
                    strDtInk);
            if (amount.getDistTp() == 1) {
                // Кис.
                Org uk = amount.getKart().getUk();
                // общий способ распределения
                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                    saveKwtpDayLog(amount, "1.0 Сумма оплаты долга > 0");

                    if (amount.getSumma().compareTo(amount.getAmntDebtDopl()) == 0) {
                        saveKwtpDayLog(amount, "2.0 Сумма оплаты = долг за период");
                        saveKwtpDayLog(amount, "2.1 Распределить по вх.деб.+кред. по списку закрытых орг, " +
                                "c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                true, false, null,
                                true, null, false);

                        saveKwtpDayLog(amount, "2.2 Распределить по вх.деб.+кред. остальные услуги, кроме " +
                                "списка закрытых орг. " +
                                "без ограничения по исх.сальдо");
                        distWithRestriction(amount, 0, false, null,
                                null, null, null,
                                false, true, null,
                                true, null, false);
                    } else if (amount.getSumma().compareTo(amount.getAmntDebtDopl()) > 0) {
                        saveKwtpDayLog(amount, "3.0 Сумма оплаты > долг за период (переплата)");
                        boolean flag = false;
                        if (uk.getDistPayTp().equals(0)) {
                            flag = true;
                            saveKwtpDayLog(amount, "3.1.1 Тип распределения - общий");
                        } else if (amount.getAmntInSal().compareTo(amount.getAmntChrgPrevPeriod()) > 0) {
                            flag = true;
                            saveKwtpDayLog(amount, "3.1.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. " +
                                    "> долг.1 мес.");
                        }
                        if (flag) {
                            saveKwtpDayLog(amount, "3.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, " +
                                    "c ограничением по исх.сал.");
                            distWithRestriction(amount, 0, true, true,
                                    true, true, true,
                                    true, false, null,
                                    true, null, false);
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "3.1.3 Распределить по начислению предыдущего периода={}, " +
                                                "без ограничения по исх.сал.",
                                        configApp.getPeriodBack());
                                distWithRestriction(amount, 3, false, null,
                                        null, null, null,
                                        false, false, null,
                                        true, null, false);
                            }
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "3.1.4 Распределить по начислению текущего периода={}, " +
                                                "без ограничения по исх.сал.",
                                        configApp.getPeriod());
                                distWithRestriction(amount, 5, false, null,
                                        null, null, null,
                                        false, false, null,
                                        true, null, false);
                            }
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "3.1.5 Распределить по вх.деб.+кред. по всем орг, " +
                                        "без ограничения по исх.сал.");
                                distWithRestriction(amount, 0, false, null,
                                        null, null, null,
                                        false, false, null,
                                        true, null, false);
                            }
                        } else {
                            saveKwtpDayLog(amount, "3.2.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. " +
                                    "<= долг.1 мес.");
                            saveKwtpDayLog(amount, "3.2.2 Распределить оплату на услугу 003");
                            distExclusivelyBySingleUslIdUk(amount, "003", true);
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "3.2.3 Остаток распределить на услугу usl=003 и УК");
                                distExclusivelyBySingleUslIdUk(amount, "003", true);
                            }
/*
                        saveKwtpDayLog(amount, "3.2.2 Распределить оплату по вх.деб.+кред. без услуги 003,
                        c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true,
                                true, false,
                                false, Collections.singletonList("003"), true);
*/
                        }
                    } else {
                        saveKwtpDayLog(amount, "4.0 Сумма оплаты < долг (недоплата)");
                        final BigDecimal rangeBegin = new BigDecimal("0.01");
                        final BigDecimal rangeEnd = new BigDecimal("100");
                        boolean flag = false;
                        if (uk.getDistPayTp() == null) {
                            throw new ErrorWhileDistPay("ОШИБКА! Не установлен тип распределения оплаты " +
                                    "по организации id="
                                    + uk.getId());
                        } else if (uk.getDistPayTp().equals(0)) {
                            flag = true;
                            saveKwtpDayLog(amount, "4.1.1 Тип распределения - общий");
                        } else if (amount.getAmntDebtDopl().subtract(amount.getSumma()).compareTo(rangeEnd) > 0) {
                            flag = true;
                            saveKwtpDayLog(amount, "4.1.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                    "> 100");
                        }
                        if (flag) {
                            saveKwtpDayLog(amount, "4.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, " +
                                    "c ограничением по исх.сал.");
                            distWithRestriction(amount, 0, true, true,
                                    true, true, true,
                                    true, false, null,
                                    true, null, false);
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "4.1.3 Распределить по вх.деб.+кред. остальные услуги, " +
                                        "кроме списка закрытых орг. " +
                                        "без ограничения по исх.сальдо");
                                distWithRestriction(amount, 0, false, null,
                                        null, null, null,
                                        false, true, null,
                                        true, null, false);
                            }
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "4.1.4 Распределить по начислению текущего периода=(), " +
                                                "без ограничения по исх.сал.",
                                        configApp.getPeriod());
                                distWithRestriction(amount, 5, false, null,
                                        null, null, null,
                                        false, false, null,
                                        true, null, false);
                            }
                        } else {
                            saveKwtpDayLog(amount, "4.2.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                    "<= 100");
                            saveKwtpDayLog(amount, "4.2.2 Распределить оплату по вх.деб.+кред. без услуги 003, " +
                                    "по списку закрытых орг, " +
                                    "c ограничением по исх.сал.");
                            distWithRestriction(amount, 0, true, true,
                                    true, true,
                                    true, true,
                                    false, Collections.singletonList("003"), true,
                                    null, false);
                            if (amount.isExistSumma()) {
                                saveKwtpDayLog(amount, "4.2.3 Распределить по вх.деб.+кред. остальные услуги, " +
                                        "кроме списка закрытых орг. и без услуги 003 " +
                                        "с ограничением по ВХ.сальдо");
                                distWithRestriction(amount, 0, false, null,
                                        null, null, null,
                                        false, true,
                                        Collections.singletonList("003"),
                                        true, null, true);
                            }
                            if (amount.isExistSumma()) {
                                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                                    saveKwtpDayLog(amount, "4.2.4 Остаток распределить на услугу usl=003");
                                    distExclusivelyBySingleUslIdUk(amount, "003", true);
                                }
                            }
                        }
                    }

                    if (amount.isExistSumma()) {
                        // во всех вариантах распределения остались нераспределенными средства
                        if (amount.getKart().getTp().getCd().equals("LSK_TP_MAIN")) {
                            // основной счет
                            saveKwtpDayLog(amount, "4.3.1 Сумма оплаты не была распределена, распределить " +
                                    "на услугу usl=003");
                            distExclusivelyBySingleUslIdUk(amount, "003", true);
                        } else {
                            // прочие типы счетов
                            saveKwtpDayLog(amount, "4.3.1 Сумма оплаты не была распределена, распределить " +
                                    "на первую услугу в наборе");
                            distExclusivelyByFirstUslId(amount, true);
                            if (amount.isExistSumma()) {
                                // если всё же осталась нераспределенная сумма, то отправить на 003 усл и УК
                                saveKwtpDayLog(amount, "4.3.2 Сумма оплаты не была распределена, распределить " +
                                        "на услугу usl=003");
                                distExclusivelyBySingleUslIdUk(amount, "003", true);
                            }
                        }
                    }

                    // выполнить редирект оплаты
                    List<SumUslOrgDTO> lstDistPay = amount.getLstDistPayment();
                    lstDistPay.forEach(t -> {
                        UslOrg uslOrgChanged =
                                referenceMng.getUslOrgRedirect(t.getUslId(), t.getOrgId(), amount.getKart(), 1);
                        boolean isRedirected = false;
                        if (!t.getUslId().equals(uslOrgChanged.getUslId())) {
                            isRedirected = true;
                            saveKwtpDayLog(amount, "4.4.0 Выполнено перенаправление оплаты: услуга " +
                                    "{}-->{}", t.getUslId(), uslOrgChanged.getUslId());
                            t.setUslId(uslOrgChanged.getUslId());
                        }
                        if (!t.getOrgId().equals(uslOrgChanged.getOrgId())) {
                            isRedirected = true;
                            saveKwtpDayLog(amount, "4.4.1 Выполнено перенаправление оплаты: организация " +
                                    "{}-->{}", t.getOrgId(), uslOrgChanged.getOrgId());
                            t.setOrgId(uslOrgChanged.getOrgId());
                        }
                        if (isRedirected) {
                            saveKwtpDayLog(amount, "4.4.2 Перенаправлена сумма={}", t.getSumma());
                        }
                    });

                } else if (amount.getSumma().compareTo(BigDecimal.ZERO) < 0) {
                    saveKwtpDayLog(amount, "2.0 Сумма оплаты < 0, снятие ранее принятой оплаты");
                    // сумма оплаты < 0 (снятие оплаты)
                    throw new ErrorWhileDistPay("ОШИБКА! Сумма оплаты < 0, операция не доступна!");
                }

                if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                    // распределение пени
                    saveKwtpDayLog(amount, "5.0 Сумма пени > 0");
                    saveKwtpDayLog(amount, "5.0.1 Распределить по уже имеющемуся распределению оплаты");
                    distWithRestriction(amount, 4, false, null,
                            null, null, null,
                            false, false, null, false,
                            null, false);
                    if (amount.isExistPenya()) {
                        saveKwtpDayLog(amount, "5.0.2 Остаток распределить по начислению");
                        distWithRestriction(amount, 5, false, null,
                                null, null, null,
                                false, false, null,
                                false, null, false);
                        if (amount.isExistPenya()) {
                            saveKwtpDayLog(amount, "5.0.3 Остаток распределить по вх.сал.пени");
                            distWithRestriction(amount, 6, false, null,
                                    null, null, null,
                                    false, false, null,
                                    false, null, false);
                        }
                        if (amount.isExistPenya()) {
                            if (amount.getKart().getTp().getCd().equals("LSK_TP_MAIN")) {
                                // основной счет
                                saveKwtpDayLog(amount, "5.1.0 Сумма пени не была распределена, распределить " +
                                        "на услугу usl=003");
                                distExclusivelyBySingleUslIdUk(amount, "003", false);
                            } else {
                                // прочие типы счетов
                                saveKwtpDayLog(amount, "5.1.0 Сумма пени не была распределена, распределить " +
                                        "на первую услугу в наборе");
                                distExclusivelyByFirstUslId(amount, true);
                                if (amount.isExistPenya()) {
                                    // если всё же осталась нераспределенная сумма, то отправить на 003 усл и УК
                                    saveKwtpDayLog(amount, "5.1.1 Сумма пени не была распределена, распределить" +
                                            " на услугу usl=003");
                                    distExclusivelyBySingleUslIdUk(amount, "003", false);
                                }
                            }
                        }
                    }
                    // выполнить редирект пени
                    List<SumUslOrgDTO> lstDistPenya = amount.getLstDistPenya();
                    lstDistPenya.forEach(t -> {
                        UslOrg uslOrgChanged =
                                referenceMng.getUslOrgRedirect(t.getUslId(), t.getOrgId(), amount.getKart(), 0);
                        boolean isRedirected = false;
                        if (!t.getUslId().equals(uslOrgChanged.getUslId())) {
                            isRedirected = true;
                            saveKwtpDayLog(amount, "5.1.0 Выполнено перенаправление пени: услуга " +
                                    "{}-->{}", t.getUslId(), uslOrgChanged.getUslId());
                            t.setUslId(uslOrgChanged.getUslId());
                        }
                        if (!t.getOrgId().equals(uslOrgChanged.getOrgId())) {
                            isRedirected = true;
                            saveKwtpDayLog(amount, "5.1.1 Выполнено перенаправление пени: организация " +
                                    "{}-->{}", t.getOrgId(), uslOrgChanged.getOrgId());
                            t.setOrgId(uslOrgChanged.getOrgId());
                        }
                        if (isRedirected) {
                            saveKwtpDayLog(amount, "5.1.2 Перенаправлена сумма={}", t.getSumma());
                        }
                    });

                } else if (amount.getPenya().compareTo(BigDecimal.ZERO) < 0) {
                    // сумма пени < 0 (снятие оплаты)
                    throw new ErrorWhileDistPay("ОШИБКА! Сумма пени < 0, операция не доступна!");
                }
            } else if (amount.getDistTp() == 2) {
                // ТСЖ
                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                    saveKwtpDayLog(amount, "1.0 Сумма оплаты долга > 0");
                    if (Integer.parseInt(amount.getDopl()) >= Integer.parseInt(configApp.getPeriod())) {
                        // текущий или будущий период (аванс) распределить точно по выбранным услугам,
                        // не превышая текущего начисления с учетом текущей оплаты
                        saveKwtpDayLog(amount, "2.0 Период оплаты >= текущий период, распределить " +
                                "по тек.начислению списка услуг, точно");
                        distWithRestriction(amount, 7, false, null,
                                null, null, null,
                                false, false, null,
                                true, Arrays.asList("011", "012", "015", "016", "013", "014", "038", "039",
                                        "033", "034", "042", "044", "045"), false);
                        if (amount.isExistSumma()) {
                            // если всё еще есть остаток суммы, распределить по прочим услугам
                            saveKwtpDayLog(amount, "2.1 Остаток распределить пропорционально начисления " +
                                    "прочих услуг, без ограничений");
                            distWithRestriction(amount, 5, false, null,
                                    null, null, null,
                                    false, false,
                                    Arrays.asList("011", "012", "015", "016", "013", "014", "038", "039",
                                            "033", "034", "042", "044", "045"), true,
                                    null, false);
                        }
                        if (amount.isExistSumma()) {
                            // если всё еще есть остаток суммы, распределить по 003 услуге
                            saveKwtpDayLog(amount, "2.2 Остаток суммы распределить на услугу usl=003");
                            distExclusivelyBySingleUslIdUk(amount, "003", true);
                        }
                    } else {
                        // предыдущий период
                        saveKwtpDayLog(amount, "3.1 Распределить по начислению предыдущего периода={}, " +
                                        "без ограничения по исх.сал.",
                                configApp.getPeriodBack());
                        distWithRestriction(amount, 3, false, null,
                                null, null, null,
                                false, false, null,
                                true, null, false);
                    }
                }

                if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                    saveKwtpDayLog(amount, "3.1 Сумму пени распределить на услугу usl=003");
                    distExclusivelyBySingleUslIdUk(amount, "003", false);
                }
            }
            // сгруппировать распределение оплаты
            List<SumUslOrgDTO> lstForKwtpDayPay = new ArrayList<>(20);
            amount.getLstDistPayment().forEach(t ->
                    saldoMng.groupByLstUslOrg(lstForKwtpDayPay, t));
            // сгруппировать распределение пени
            List<SumUslOrgDTO> lstForKwtpDayPenya = new ArrayList<>(20);
            amount.getLstDistPenya().forEach(t ->
                    saldoMng.groupByLstUslOrg(lstForKwtpDayPenya, t));
            Map<Integer, List<SumUslOrgDTO>> mapForKwtpDay = new HashMap<>();
            mapForKwtpDay.put(1, lstForKwtpDayPay);
            mapForKwtpDay.put(0, lstForKwtpDayPenya);

            // сохранить распределение в KWTP_DAY
            saveKwtpDayLog(amount, "5.3 Итоговое, сгруппированное распределение в KWTP_DAY:");

            Map<Integer, BigDecimal> mapControl = new HashMap<>();
            mapForKwtpDay.forEach((key, lstSum) -> lstSum.forEach(d -> {
                KwtpDay kwtpDay = KwtpDay.KwtpDayBuilder.aKwtpDay()
                        .withNink(amount.getNink())
                        .withNkom(amount.getNkom())
                        .withOper(amount.getOper())
                        .withUsl(em.find(Usl.class, d.getUslId()))
                        .withOrg(em.find(Org.class, d.getOrgId()))
                        .withSumma(d.getSumma())
                        .withDopl(amount.getDopl())
                        .withDt(amount.getDtek())
                        .withDtInk(amount.getDatInk())
                        .withKart(amount.getKart())
                        .withFkKwtpMg(amount.getKwtpMgId())
                        .withTp(key).build();
                if (mapControl.get(key) != null) {
                    mapControl.put(key, mapControl.get(key).add(d.getSumma()));
                } else {
                    mapControl.put(key, d.getSumma());
                }
                saveKwtpDayLog(amount, "tp={}, usl={}, org={}, summa={}",
                        kwtpDay.getTp(), kwtpDay.getUsl().getId(), kwtpDay.getOrg().getId(), d.getSumma());
                em.persist(kwtpDay); // note Используй crud.save
            }));


            log.info("5.4 Итого распределено Сумма={}, Пеня={}", mapControl.get(1), mapControl.get(0));
            if (amount.getSummaControl().compareTo(Utl.nvl(mapControl.get(1), BigDecimal.ZERO)) != 0
                    || amount.getSumma().compareTo(BigDecimal.ZERO) != 0) {
                throw new ErrorWhileDistPay("ОШИБКА! Не вся оплата распределена!");
            }
            if (amount.getPenyaControl().compareTo(Utl.nvl(mapControl.get(0), BigDecimal.ZERO)) != 0
                    || amount.getPenya().compareTo(BigDecimal.ZERO) != 0) {
                throw new ErrorWhileDistPay("ОШИБКА! Не вся пеня распределена!");
            }

        } catch (WrongParam e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileDistPay("Произошла ошибка в процессе распределения оплаты!");
        }

    }

    /**
     * Построить объект расчета
     */
    private Amount buildAmount(int kwtpMgId, boolean isGenChrg, String lsk,
                               String strSumma, String strPenya, String strDebt,
                               String dopl, int nink, String nkom, String oper,
                               String dtekStr, String datInkStr) throws WrongParam {
        Amount amount = new Amount();

        // тип распределения оплаты (0 - Нет, 1-Кис, 2- ТСЖ)
        amount.setDistTp(Utl.nvl(sprParamMng.getN1("JAVA_DIST_KWTP_MG"), 0D).intValue());

        Kart kart = em.find(Kart.class, lsk);
        amount.setKart(kart);
        amount.setSumma(strSumma != null && strSumma.length() > 0 ? new BigDecimal(strSumma) : BigDecimal.ZERO);
        amount.setPenya(strPenya != null && strPenya.length() > 0 ? new BigDecimal(strPenya) : BigDecimal.ZERO);
        // сохранить суммы для контроля распределения
        amount.setSummaControl(amount.getSumma());
        amount.setPenyaControl(amount.getPenya());

        amount.setAmntDebtDopl(strDebt != null && strDebt.length() > 0 ? new BigDecimal(strDebt) : BigDecimal.ZERO);
        amount.setKwtpMgId(kwtpMgId);
        amount.setDopl(dopl);
        amount.setNink(nink);
        amount.setNkom(nkom);
        amount.setOper(oper);
        try {
            amount.setDtek(Utl.getDateFromStr(dtekStr));
            amount.setDatInk(datInkStr != null && datInkStr.length() != 0 ? Utl.getDateFromStr(datInkStr) : null);
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            throw new WrongParam("ERROR! Некорректный период!");
        }

        saveKwtpDayLog(amount, "***** Распределение оплаты ver=1.03 *****");
        saveKwtpDayLog(amount, "1.0 C_KWTP_MG.LSK={}, C_KWTP_MG.ID={}, C_KWTP_MG.SUMMA={}, C_KWTP_MG.PENYA={}, Дата-время={}",
                kart.getLsk(), amount.getKwtpMgId(), amount.getSumma(), amount.getPenya(),
                Utl.getStrFromDate(new Date(), "dd.MM.yyyy HH:mm:ss"));
        saveKwtpDayLog(amount, "УК: {}", amount.getKart().getUk().getReu());
        saveKwtpDayLog(amount, "Тип счета: {}", amount.getKart().getTp().getName());
        saveKwtpDayLog(amount, "Тип распределения, параметр JAVA_DIST_KWTP_MG={}", amount.getDistTp());
        saveKwtpDayLog(amount, "Долг за период: {}", amount.getAmntDebtDopl());

        // сформировать начисление
        if (isGenChrg) {
            if (Utl.in(amount.getDistTp(), 1)) {
                // только вариант Кис.
                processMng.processWebRequest(0, 0, amount.getDtek(), null, null,
                        amount.getKart().getKoKw(), null, null);
                //} else {
                // ТСЖ - вызвать начисление PL/SQL - не надо! делается в Direct
                // расчитать начисление по одному лиц счету
/*
                StoredProcedureQuery qr = em.createStoredProcedureQuery("scott.c_charges.gen_charges");
                // id дома
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, String.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(3, Integer.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(4, Integer.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(5, Integer.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(6, Integer.class, ParameterMode.IN);
                qr.setParameter(1, kart.getLsk());
                qr.setParameter(2, kart.getLsk());
                qr.setParameter(3, null);
                qr.setParameter(4, null);
                qr.setParameter(5, 1);
                qr.setParameter(6, 0);
                qr.executeUpdate();
*/
            }
        }

        // получить вх.общ.сал.
        amount.setInSal(saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                null, null,
                true, false, false, false, false, false,
                null, false, false));
        // итог по вх.сал.
        amount.setAmntInSal(amount.getInSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));

        saveKwtpDayLog(amount, "Итого сумма вх.деб.+кред.сал={}", amount.getAmntInSal());
        // получить начисление за прошлый период
        List<SumUslOrgDTO> lstChrgPrevPeriod =
                saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                        null, null,
                        false, false, true, false, false, false,
                        configApp.getPeriodBack(), false, false);
        // получить итог начисления за прошлый период
        amount.setAmntChrgPrevPeriod(lstChrgPrevPeriod
                .stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        saveKwtpDayLog(amount, "Итого сумма начисления за прошлый период={}", amount.getAmntChrgPrevPeriod());

        /*saveKwtpDayLog(amount.getKwtpMg(),"Вх.сальдо по лиц.счету lsk={}:",
                amount.getKart().getLsk());
        amount.getInSal().forEach(t -> saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                t.getUslId(), t.getOrgId(), t.getSumma()));
        amount.setLstSprProcPay(sprProcPayDAO.findAll());
        saveKwtpDayLog(amount.getKwtpMg(),"итого:{}", amount.getAmntInSal());
        saveKwtpDayLog(amount.getKwtpMg(),"");*/

        // получить вх.деб.сал.
/*        amount.setInDebSal(amount.getInSal()
                .stream().filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                .collect(Collectors.toList()));
        // итог по вх.деб.сал.
        amount.setAmntInDebSal(amount.getInDebSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        saveKwtpDayLog(amount.getKwtpMg(),"Вх.деб.сальдо по лиц.счету lsk={}:",
                amount.getKart().getLsk());
        amount.getInDebSal().forEach(t -> saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                t.getUslId(), t.getOrgId(), t.getSumma()));*/
        amount.setLstSprProcPay(sprProcPayDAO.findAll());
        //saveKwtpDayLog(amount.getKwtpMg(),"итого:{}", amount.getAmntInDebSal());
        return amount;
    }

    /**
     * Распределить платеж
     *  @param amount                   - итоги
     * @param tp                       - тип 0-по вх.деб.сал.+кред.сал, 1- по начислению,
     *                                 2- по деб.сал-оплата, 3 -по начислению заданного периода
     *                                 4- по уже готовому распределению оплаты долга (распр.пени обычно)
     *                                 5- по начислению текущего периода,
     *                                 6- распределение пени по вх.саль.до по пене
     *                                 7- распределение оплаты точно, без округлений по списку услуг
     * @param isRestrictByOutSal       - ограничить по исх.сал. (проверять чтобы не создавалось кред.сальдо)?
     * @param isUseChargeInRestrict    - использовать в ограничении по исх.деб.сал.начисление?
     * @param isUseChangeInRestrict    - использовать в ограничении по исх.деб.сал.перерасчеты?
     * @param isUseCorrPayInRestrict   - использовать в ограничении по исх.деб.сал.корр оплаты?
     * @param isUsePayInRestrict       - использовать в ограничении по исх.деб.сал.оплату?
     * @param isIncludeByClosedOrgList - включая услуги и организации по списку закрытых организаций?
     * @param isExcludeByClosedOrgList - исключая услуги и организации по списку закрытых организаций?
     * @param lstExcludeUslId          - список Id услуг, которые исключить из базовой коллекции для распределения
     * @param isDistPay                - распределять оплату - да, пеню - нет
     * @param lstFilterByUslId         - профильтровать по списку Id услуг
     * @param isRestrictByInSal        - ограничить по исх.сал. (проверять чтобы не создавалось кред.сальдо)?
     */
    private void distWithRestriction(Amount amount, int tp, boolean isRestrictByOutSal,
                                     Boolean isUseChargeInRestrict, Boolean isUseChangeInRestrict,
                                     Boolean isUseCorrPayInRestrict, Boolean isUsePayInRestrict,
                                     boolean isIncludeByClosedOrgList,
                                     boolean isExcludeByClosedOrgList, List<String> lstExcludeUslId,
                                     boolean isDistPay,
                                     List<String> lstFilterByUslId, boolean isRestrictByInSal) throws WrongParam {
        if (!isRestrictByOutSal && isUseChargeInRestrict != null && isUseChangeInRestrict != null
                && isUseCorrPayInRestrict != null && isUsePayInRestrict != null) {
            throw new WrongParam("Некорректно заполнять isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=false!");
        } else if (isRestrictByOutSal && isRestrictByInSal) {
            throw new WrongParam("Некорректно использовать совместно isRestrictByOutSal=true и isRestrictByInSal=true!");
        } else if (isRestrictByInSal  && (isUseChargeInRestrict != null || isUseChangeInRestrict != null
                || isUseCorrPayInRestrict != null || isUsePayInRestrict != null)) {
            throw new WrongParam("Некорректно использовать совместно isRestrictByInSal=true и isUseChargeInRestrict, " +
                    "isUseChangeInRestrict, isUseCorrPayInRestrict, isUsePayInRestrict!");
        } else if (isRestrictByOutSal && (isUseChargeInRestrict == null && isUseChangeInRestrict == null
                && isUseCorrPayInRestrict == null && isUsePayInRestrict == null)) {
            throw new WrongParam("Не заполнено isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=true!");
        }

        // Распределить на все услуги
        String currPeriod = configApp.getPeriod();
        List<SumUslOrgDTO> lstDistribBase;
        // получить базовую коллекцию для распределения
        if (tp == 0) {
            // получить вх.сал.
            lstDistribBase = new ArrayList<>(amount.getInSal());
        } else if (tp == 1) {
            // получить начисление
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, true, false, false,
                    false, false, null, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp == 3) {
            // получить начисление предыдущего периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, false, true, false, false, false,
                    configApp.getPeriodBack(), false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp == 4) {
            // получить уже распределенную сумму оплаты, в качестве базы для распределения (обычно распр.пени)
            lstDistribBase = amount.getLstDistPayment();
        } else if (tp == 5) {
            // получить начисление текущего периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    null, null,
                    false, true, false, false, false, false,
                    null, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp == 6) {
            // получить вх.сальдо по пене
            lstDistribBase = saldoUslDAO.getPinSalXitog3ByLsk(amount.getKart().getLsk(), currPeriod)
                    .stream().map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma()))
                    .collect(Collectors.toList());
        } else if (tp == 7) {
            // получить текущее начисление ред. 25.06.2019 убрал вот это:минус оплата за текущий период и корректировки по оплате за текущий период
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    null, null,
                    false, true, false, false, false,
                    false, null, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else {
            throw new WrongParam("Некорректный параметр tp=" + tp);
        }
        // исключить нули
        lstDistribBase.removeIf(t -> t.getSumma().compareTo(BigDecimal.ZERO) == 0);

        // исключить услуги
        if (lstExcludeUslId != null) {
            lstDistribBase.removeIf(t -> lstExcludeUslId.contains(t.getUslId()));
        }
        // оставить только услуги по списку
        if (lstFilterByUslId != null) {
            lstDistribBase.removeIf(t -> !lstFilterByUslId.contains(t.getUslId()));
        }

        if (isIncludeByClosedOrgList) {
            // оставить только услуги и организации, содержащиеся в списке закрытых орг.
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().noneMatch(d -> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())  // организация - поставщик
                            && Utl.between2(amount.getDopl(), d.getMgFrom(), d.getMgTo()) // период
                    )
            );
        } else if (isExcludeByClosedOrgList) {
            // оставить только услуги и организации, НЕ содержащиеся в списке закрытых орг.
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().anyMatch(d -> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())  // организация - поставщик
                            && Utl.between2(amount.getDopl(), d.getMgFrom(), d.getMgTo()) // период
                    )
            );
        } else
            //noinspection ConstantConditions
            if (isIncludeByClosedOrgList && isExcludeByClosedOrgList) {
                throw new WrongParam("Некорректно использовать isIncludeByClosedOrgList=true и " +
                        "isExcludeByClosedOrgList=true одновременно!");
            }

        BigDecimal amntSal = lstDistribBase.stream()
                .map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        if (Utl.in(tp, 1, 7)) {
            saveKwtpDayLog(amount, "Выбранное текущее начисление > 0 для распределения оплаты, по лиц.счету lsk={}:",
                    amount.getKart().getLsk());
        }
        saveKwtpDayLog(amount, "Будет распределено по строкам:");
        lstDistribBase.forEach(t ->
                saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
        saveKwtpDayLog(amount, "итого:{}", amntSal);

        Map<DistributableBigDecimal, BigDecimal> mapDistPay = null;
        if (tp == 7) {
            // распределить сумму в точности по услугам
            if (isDistPay) {
                // распределить оплату
                mapDistPay = Utl.distBigDecimalPositiveByListIntoMapExact(amount.getSumma(), lstDistribBase);
            } else {
                // распределить пеню (не должна быть в данном типе распр, но вдруг будет)
                mapDistPay = Utl.distBigDecimalPositiveByListIntoMapExact(amount.getPenya(), lstDistribBase);
            }
        } else {
            // прочие типы распределения
            // распределить сумму по базе распределения
            if (isDistPay) {
                // распределить оплату
                mapDistPay =
                        Utl.distBigDecimalByListIntoMap(amount.getSumma(), lstDistribBase, 2);
            } else {
                // распределить пеню
                mapDistPay =
                        Utl.distBigDecimalByListIntoMap(amount.getPenya(), lstDistribBase, 2);
            }
        }

        // распечатать предварительное распределение оплаты или сохранить, если не будет ограничения по сальдо
        BigDecimal distSumma = saveDistPay(amount, mapDistPay, !(isRestrictByOutSal || isRestrictByInSal), isDistPay);

        if (isRestrictByOutSal || isRestrictByInSal) {
            if (isRestrictByOutSal) {
                saveKwtpDayLog(amount, "Сумма для распределения будет ограничена по исх.сальдо");
            } else {
                saveKwtpDayLog(amount, "Сумма для распределения будет ограничена по вх.сальдо");
            }
            if (distSumma.compareTo(BigDecimal.ZERO) != 0) {
                // Ограничить суммы  распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> lstOutSal = null;
                if (isRestrictByOutSal) {
                    lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                            amount.getLstDistPayment(), amount.getLstDistControl(),
                            true, isUseChargeInRestrict, false, isUseChangeInRestrict,
                            isUseCorrPayInRestrict,
                            isUsePayInRestrict, null, false, false);
                } else {
                    lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                            amount.getLstDistPayment(), amount.getLstDistControl(),
                            true, false, false, false,
                            false,false, null, false, false);
                }
                if (isRestrictByOutSal) {
                    saveKwtpDayLog(amount, "Исх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                } else {
                    saveKwtpDayLog(amount, "Вх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                }
                lstOutSal.forEach(t -> saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));

                // ограничить суммы распределения по исх. или вх. сал.
                for (Map.Entry<DistributableBigDecimal, BigDecimal> dist : mapDistPay.entrySet()) {
                    SumUslOrgDTO distRec = (SumUslOrgDTO) dist.getKey();
                    if (dist.getValue().compareTo(BigDecimal.ZERO) != 0) {
                        // контролировать по значениям распределения
                        lstOutSal.stream().filter(sal -> sal.getUslId().equals(distRec.getUslId())
                                && sal.getOrgId().equals(distRec.getOrgId()))
                                .forEach(sal -> {
                                    if (dist.getValue().compareTo(BigDecimal.ZERO) > 0) {
                                        // ограничить положительные суммы распределения, если появилось кред.сал.
                                        if (sal.getSumma().compareTo(BigDecimal.ZERO) < 0) {
                                            if (sal.getSumma().abs().compareTo(dist.getValue()) > 0) {
                                                // кредит сальдо больше распред.суммы в абс выражении
                                                dist.setValue(BigDecimal.ZERO);
                                            } else {
                                                // кредит сальдо меньше распред.суммы в абс выражении
                                                dist.setValue(dist.getValue().subtract(sal.getSumma().abs()));
                                            }
                                        }
                                    } else {
                                        // ограничить отрицательные суммы распределения, если появилось деб.сал.
                                        if (sal.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                                            if (sal.getSumma().compareTo(dist.getValue().abs()) > 0) {
                                                // деб.сальдо больше распред.суммы в абс выражении
                                                dist.setValue(BigDecimal.ZERO);
                                            } else {
                                                // деб.сальдо меньше распред.суммы в абс выражении
                                                dist.setValue(dist.getValue().abs().subtract(sal.getSumma()).negate());
                                            }
                                        }
                                    }

                                    if (isRestrictByOutSal) {
                                        saveKwtpDayLog(amount,
                                                "распределение ограничено по исх.сал. usl={}, org={}, summa={}",
                                                sal.getUslId(), sal.getOrgId(), dist.getValue());
                                    } else {
                                        saveKwtpDayLog(amount,
                                                "распределение ограничено по вх.сал. usl={}, org={}, summa={}",
                                                sal.getUslId(), sal.getOrgId(), dist.getValue());
                                    }
                                });
                    }
                }

                // сохранить, распечатать распределение оплаты
                distSumma = saveDistPay(amount, mapDistPay, true, isDistPay);
                // получить, распечатать исходящее сальдо
                lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                        amount.getLstDistPayment(), amount.getLstDistPayment(),
                        true, true, false, true,
                        true, true, null, false, false);
                if (isDistPay) {
                    saveKwtpDayLog(amount, "После распределения оплаты: Исх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                }
                lstOutSal.forEach(t -> saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
            }
        }

        // вычесть из итоговой суммы платежа
        if (isDistPay) {
            amount.setSumma(amount.getSumma().add(distSumma.negate()));
            saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        } else {
            amount.setPenya(amount.getPenya().add(distSumma.negate()));
            saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getPenya());
        }


    }

    /**
     * Распределить платеж экслюзивно, на одну услугу
     *
     * @param amount           - итоги
     * @param includeOnlyUslId - список Id услуг, на которые распределить оплату, не зависимо от их сальдо
     * @param isDistPay        - распределять оплату - да, пеню - нет
     */
/*
    private void distExclusivelyBySingleUslId(Amount amount, String includeOnlyUslId, boolean isDistPay) throws ErrorWhileDistPay {
        // Распределить эксклюзивно на одну услугу
        Nabor nabor = naborDAO.getByLskUsl(amount.getKart().getLsk(), includeOnlyUslId);
        if (nabor != null) {
            // сохранить для записи в KWTP_DAY
            BigDecimal distSumma;
            if (isDistPay) {
                distSumma = amount.getSumma();
                amount.getLstDistPayment().add(
                        new SumUslOrgDTO(includeOnlyUslId, nabor.getOrg().getId(), distSumma));
                amount.setSumma(amount.getSumma().add(distSumma.negate()));
            } else {
                distSumma = amount.getPenya();
                amount.getLstDistPenya().add(
                        new SumUslOrgDTO(includeOnlyUslId, nabor.getOrg().getId(), distSumma));
                amount.setPenya(amount.getPenya().add(distSumma.negate()));
            }
            saveKwtpDayLog(amount, "Распределено фактически:");
            saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                    includeOnlyUslId, nabor.getOrg().getId(), distSumma);
            saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        } else {
            throw new ErrorWhileDistPay("При распределении не найдена запись в наборе услуг lsk="
                    + amount.getKart().getLsk()
                    + ", usl=" + includeOnlyUslId);
        }
    }
*/

    /**
     * Распределить платеж экслюзивно, на первую услугу в наборе
     *
     * @param amount    - итоги
     * @param isDistPay - распределять оплату - да, пеню - нет
     */
    private void distExclusivelyByFirstUslId(Amount amount, boolean isDistPay) throws ErrorWhileDistPay {
        Nabor nabor = amount.getKart().getNabor().stream().findFirst().orElse(null);
        if (nabor != null) {
            // сохранить для записи в KWTP_DAY
            BigDecimal distSumma;
            if (isDistPay) {
                distSumma = amount.getSumma();
                amount.getLstDistPayment().add(
                        new SumUslOrgDTO(nabor.getUsl().getId(), nabor.getOrg().getId(), distSumma));
                amount.setSumma(amount.getSumma().add(distSumma.negate()));
            } else {
                distSumma = amount.getPenya();
                amount.getLstDistPenya().add(
                        new SumUslOrgDTO(nabor.getUsl().getId(), nabor.getOrg().getId(), distSumma));
                amount.setPenya(amount.getPenya().add(distSumma.negate()));
            }
            saveKwtpDayLog(amount, "Распределено фактически:");
            saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                    nabor.getUsl().getId(), nabor.getOrg().getId(), distSumma);
            saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        }
    }

    /**
     * Распределить платеж экслюзивно, на одну услугу и УК лицевого счета
     *
     * @param amount           - итоги
     * @param includeOnlyUslId - список Id услуг, на которые распределить оплату, не зависимо от их сальдо
     * @param isDistPay        - распределять оплату - да, пеню - нет
     */
    private void distExclusivelyBySingleUslIdUk(Amount amount, String includeOnlyUslId,
                                                boolean isDistPay) {
        // сохранить для записи в KWTP_DAY
        BigDecimal distSumma;
        if (isDistPay) {
            distSumma = amount.getSumma();
            amount.getLstDistPayment().add(
                    new SumUslOrgDTO(includeOnlyUslId, amount.getKart().getUk().getId(), distSumma));
            amount.setSumma(amount.getSumma().add(distSumma.negate()));
        } else {
            distSumma = amount.getPenya();
            amount.getLstDistPenya().add(
                    new SumUslOrgDTO(includeOnlyUslId, amount.getKart().getUk().getId(), distSumma));
            amount.setPenya(amount.getPenya().add(distSumma.negate()));
        }
        saveKwtpDayLog(amount, "Распределено фактически:");
        saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                includeOnlyUslId, amount.getKart().getUk().getId(), distSumma);
        saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                distSumma, amount.getSumma());
    }

    /**
     * Сохранить, суммировать и распечатать распределение
     *
     * @param amount     - Итоги
     * @param mapDistPay - коллекция распределения
     * @param isSave     - сохранить распределение?
     * @param isDistPay  - распределить оплату - да, пеню - нет
     */
    private BigDecimal saveDistPay(Amount amount, Map<DistributableBigDecimal, BigDecimal> mapDistPay,
                                   boolean isSave, boolean isDistPay) {
        String msg = "оплаты";
        if (!isDistPay) {
            msg = "пени";
        }
        if (isSave) {
            saveKwtpDayLog(amount, "Распределено {}, фактически:", msg);
        } else {
            saveKwtpDayLog(amount, "Распределено {}, предварительно:", msg);
        }
        BigDecimal amnt = BigDecimal.ZERO;
        for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay.entrySet()) {
            SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) t.getKey();
            if (isSave) {
                // сохранить для записи в KWTP_DAY
                if (isDistPay) {
                    // оплата
                    amount.getLstDistPayment().add(
                            new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
                } else {
                    // пеня
                    amount.getLstDistPenya().add(
                            new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
                }
            } else {
                // сохранить для контроля
                amount.getLstDistControl().add(
                        new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
            }
            amnt = amnt.add(t.getValue());
            saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                    sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
        }

        saveKwtpDayLog(amount, "итого {}, распределено:{}", msg, amnt);
        return amnt;
    }

    /**
     * Выполняемая раз в месяц коррекционная проводка по сальдо
     */
    @Override
    public void distSalCorrOperation() {
        //configApp.getPeriod();
        saldoUslDAO.getSaldoUslBySign("201404", 1)
                .forEach(t -> log.info("SaldoUsl: lsk={}, usl={}, org={}, summa={}",
                        t.getKart().getLsk(), t.getUsl().getId(),
                        t.getOrg().getId(), t.getSumma()));


    }

    /**
     * Сохранить сообщение в лог KWTP_DAY_LOG
     *
     * @param amount - итоги распределения
     * @param msg    - сообщение
     * @param t      - параметры
     */
    private void saveKwtpDayLog(Amount amount, String msg, Object... t) {
        int kwtpMgId = amount.getKwtpMgId();
        KwtpDayLog kwtpDayLog =
                KwtpDayLog.KwtpDayLogBuilder.aKwtpDayLog()
                        .withNpp(amount.getNpp())
                        .withFkKwtpMg(kwtpMgId)
                        .withText(Utl.getStrUsingTemplate(msg, t)).build();
        amount.setNpp(amount.getNpp() + 1);
        em.persist(kwtpDayLog); // note Используй crud.save
        log.info(msg, t);
    }
}
