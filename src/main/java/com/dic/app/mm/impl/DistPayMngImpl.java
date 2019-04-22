package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.bill.dao.NaborDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.SprProcPayDAO;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import com.ric.cmn.excp.WrongParam;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

/**
 * Сервис распределения оплаты
 */
@Slf4j
@Service
public class DistPayMngImpl implements DistPayMng {

    private final SaldoMng saldoMng;
    private final ConfigApp configApp;
    private final SprProcPayDAO sprProcPayDAO;
    private final NaborDAO naborDAO;
    private final SaldoUslDAO saldoUslDAO;
    @PersistenceContext
    private EntityManager em;

    public DistPayMngImpl(SaldoMng saldoMng, ConfigApp configApp,
                          SprProcPayDAO sprProcPayDAO, NaborDAO naborDAO, SaldoUslDAO saldoUslDAO) {
        this.saldoMng = saldoMng;
        this.configApp = configApp;
        this.sprProcPayDAO = sprProcPayDAO;
        this.naborDAO = naborDAO;
        this.saldoUslDAO = saldoUslDAO;
    }

    /**
     * Класс итогов распределения
     */
    @Getter
    @Setter
    private class Amount {
        // строка платежа
        private KwtpMg kwtpMg;
        // лиц.счет
        private Kart kart;
        // сумма оплаты
        private BigDecimal summa = BigDecimal.ZERO;
        // сумма оплаченной пени
        private BigDecimal penya = BigDecimal.ZERO;
        // входящее, общее сальдо
        private List<SumUslOrgDTO> inSal;
        // итог по общ.сал.
        private BigDecimal amntInSal = BigDecimal.ZERO;
        // итог по начислению предыдущего периода
        private BigDecimal amntChrgPrevPeriod = BigDecimal.ZERO;

        // входящее, дебетовое сальдо
        //private List<SumUslOrgDTO> inDebSal;
        // итог по вх.деб.сал.
        //private BigDecimal amntInDebSal = BigDecimal.ZERO;
        // список закрытых орг.
        List<SprProcPay> lstSprProcPay;
        // уже распределенная оплата
        private List<SumUslOrgDTO> lstDistPayment = new ArrayList<>();
        // уже распределенная оплата - для контроля
        private List<SumUslOrgDTO> lstDistControl = new ArrayList<>();
    }

    /**
     * Распределить платеж (запись в C_KWTP_MG)
     * @param kwtpMgId - ID записи C_KWTP_MG
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class) //
    public void distKwtpMg(int kwtpMgId) throws ErrorWhileDistPay {
        KwtpMg kwtpMg = em.find(KwtpMg.class, kwtpMgId);
        assertNotNull(kwtpMg);
        saveKwtpDayLog(kwtpMg,"***** Распределение оплаты *****");
        try {
            // очистить текущее распределение
            kwtpMg.getKwtpDay().clear();

            Amount amount = buildAmount(kwtpMg);
            Org uk = amount.getKart().getUk();
            // общий способ распределения
            if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                saveKwtpDayLog(amount.getKwtpMg(),"1.0 Сумма оплаты > 0");

                if (amount.getSumma().compareTo(amount.amntInSal) == 0) {
                    saveKwtpDayLog(amount.getKwtpMg(),"2.0 Сумма оплаты = вх.деб.+кред.");
                    saveKwtpDayLog(amount.getKwtpMg(),"2.1 Распределить по вх.деб.+кред. по списку закрытых орг, c ограничением по исх.сал.");
                    distWithRestriction(amount, 0, true, true,
                            true, true, true,
                            true, false, null);

                    saveKwtpDayLog(amount.getKwtpMg(),"2.2 Распределить по вх.деб.+кред. остальные услуги, кроме списка закрытых орг. " +
                            "без ограничения по исх.сальдо");
                    distWithRestriction(amount, 0, false, null,
                            null, null, null,
                            false, true, null);
                } else if (amount.getSumma().compareTo(amount.amntInSal) > 0) {
                    saveKwtpDayLog(amount.getKwtpMg(),"3.0 Сумма оплаты > вх.деб.+кред. (переплата)");
                    boolean flag = false;
                    if (uk.getDistPayTp().equals(0)) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMg(),"3.1.1 Тип распределения - общий");
                    } else if (amount.getAmntInSal().compareTo(amount.getAmntChrgPrevPeriod()) > 0) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMg(),"3.1.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. > долг.1 мес.");
                    }
                    if (flag) {
                        saveKwtpDayLog(amount.getKwtpMg(),"3.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                true, false, null);

                        saveKwtpDayLog(amount.getKwtpMg(),"3.1.3 Распределить по начислению предыдущего периода={}, без ограничения по исх.сал.",
                                configApp.getPeriodBack());
                        distWithRestriction(amount, 3, false, null,
                                null, null, null,
                                false, false, null);
                    } else {
                        saveKwtpDayLog(amount.getKwtpMg(),"3.2.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. <= долг.1 мес.");

                        saveKwtpDayLog(amount.getKwtpMg(),"3.2.2 Распределить оплату по вх.деб.+кред. без услуги 003, c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true,
                                true, false,
                                false, Collections.singletonList("003"));
                        if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                            saveKwtpDayLog(amount.getKwtpMg(),"3.2.3 Остаток распределить на услугу usl=003 ");
                            distExclusivelyBySingleUslId(amount, "003");
                        }
                    }
                } else {
                    saveKwtpDayLog(amount.getKwtpMg(),"4.0 Сумма оплаты < долг (недоплата)");
                    final BigDecimal rangeBegin = new BigDecimal("0.01");
                    final BigDecimal rangeEnd = new BigDecimal("100");
                    boolean flag = false;
                    if (uk.getDistPayTp().equals(0)) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMg(),"4.1.1 Тип распределения - общий");
                    } else if (amount.getAmntInSal().subtract(amount.getSumma()).compareTo(rangeEnd) > 0) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMg(),"4.1.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                "> 100");
                    }
                    if (flag) {
                        saveKwtpDayLog(amount.getKwtpMg(),"4.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                true, false, null);

                        saveKwtpDayLog(amount.getKwtpMg(),"4.1.3 Распределить по вх.деб.+кред. остальные услуги, кроме списка закрытых орг. " +
                                "без ограничения по исх.сальдо");
                        distWithRestriction(amount, 0, false, null,
                                null, null, null,
                                false, true, null);
                    } else {
                        saveKwtpDayLog(amount.getKwtpMg(),"4.2.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                "<= 100");
                        saveKwtpDayLog(amount.getKwtpMg(),"4.2.2 Распределить оплату по вх.деб.+кред. без услуги 003, по списку закрытых орг, " +
                                "c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true,
                                true, true,
                                false, Collections.singletonList("003"));

                        saveKwtpDayLog(amount.getKwtpMg(),"4.2.3 Распределить по вх.деб.+кред. остальные услуги, кроме списка закрытых орг. " +
                                "с ограничением по исх.сальдо");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                false, true, null);
                        if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                            saveKwtpDayLog(amount.getKwtpMg(),"4.2.4 Остаток распределить на услугу usl=003 ");
                            distExclusivelyBySingleUslId(amount, "003");
                        }
                    }
                }
            } else {
                saveKwtpDayLog(amount.getKwtpMg(),"2.0 Сумма оплаты < 0, снятие ранее принятой оплаты");
                // сумма оплаты < 0 (снятие оплаты)
                // TODO ????? должно выполняться в отдельной процедуре? (уже есть такое в PL/SQL, написано нормально
            }

            // TODO сделать распределение пени!!!!

            // сгруппировать распределение
            List<SumUslOrgDTO> lstForKwtpDay = new ArrayList<>(20);
            amount.getLstDistPayment().forEach(t ->
                    saldoMng.groupByUslOrg(lstForKwtpDay, t));
            // сохранить распределение в KWTP_DAY
            lstForKwtpDay.forEach(t -> {
                // оплата
                KwtpDay kwtpDay = KwtpDay.KwtpDayBuilder.aKwtpDay()
                        .withNink(kwtpMg.getNink())
                        .withNkom(kwtpMg.getNkom())
                        .withOper(kwtpMg.getOper())
                        .withUsl(em.find(Usl.class, t.getUslId()))
                        .withOrg(em.find(Org.class, t.getOrgId()))
                        .withSumma(t.getSumma())
                        .withDopl(kwtpMg.getDopl())
                        .withDt(kwtpMg.getDt())
                        .withDtInk(kwtpMg.getDtInk())
                        .withKart(kwtpMg.getKart())
                        .withKwtpMg(kwtpMg)
                        .withTp(1).build();
                // TODO пеня?????????
                kwtpMg.getKwtpDay().add(kwtpDay);
                em.persist(kwtpDay);
            });

            saveKwtpDayLog(amount.getKwtpMg(),"5.0 Итоговое, сгруппированное распределение в KWTP_DAY:");
            kwtpMg.getKwtpDay().forEach(t ->
                    saveKwtpDayLog(amount.getKwtpMg(),"tp={}, usl={}, org={}, summa={}",
                            t.getTp(), t.getUsl().getId(), t.getOrg().getId(), t.getSumma())
            );
        } catch (WrongParam wrongParam) {
            throw new ErrorWhileDistPay("Произошла ошибка в процессе распределения оплаты!");
        }
    }


    /**
     * Построить объект расчета
     *
     * @param kwtpMg - строка платежа
     */
    private Amount buildAmount(KwtpMg kwtpMg) throws WrongParam {
        // note Выполнить начисление предварительно?
        Amount amount = new Amount();
        amount.setKwtpMg(kwtpMg);
        amount.setKart(kwtpMg.getKart());
        amount.setSumma(Utl.nvl(kwtpMg.getSumma(), BigDecimal.ZERO));
        amount.setPenya(Utl.nvl(kwtpMg.getPenya(), BigDecimal.ZERO));
        saveKwtpDayLog(amount.getKwtpMg(),"1.0 C_KWTP_MG.ID={}, C_KWTP_MG.SUMMA={}, C_KWTP_MG.PENYA={}",
                kwtpMg.getId(), amount.getSumma(), amount.getPenya());
        saveKwtpDayLog(amount.getKwtpMg(),"    KART.REU={}", amount.getKart().getUk().getReu());
        // получить вх.общ.сал.
        amount.setInSal(saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                null, null,
                true, false, false, false, false, false, null));
        // итог по вх.сал.
        amount.setAmntInSal(amount.getInSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        saveKwtpDayLog(amount.getKwtpMg(),"Итого вх.сал.={}", amount.getAmntInSal());
        // получить начисление за прошлый период
        List<SumUslOrgDTO> lstChrgPrevPeriod =
                saldoMng.getOutSal(amount.getKart(), configApp.getPeriodBack(),
                        null, null,
                        false, true, false, true, true, true, null);
        // получить итог начисления за прошлый период
        amount.setAmntChrgPrevPeriod(lstChrgPrevPeriod
                .stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        saveKwtpDayLog(amount.getKwtpMg(),"Итого сумма нач.за прошлый период={}", amount.getAmntChrgPrevPeriod());

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
     *
     * @param amount                   - итоги
     * @param tp                       - тип 0-по вх.деб.сал.+кред.сал, 1- по начислению+перерасчет-оплата,
     *                                 2- по деб.сал-оплата, 3 -по начислению заданного периода
     * @param isRestrictByOutSal       - ограничить по исх.сал. (проверять чтобы не создавалось кред.сальдо)?
     * @param isUseChargeInRestrict    - использовать в ограничении по исх.деб.сал.начисление?
     * @param isUseChangeInRestrict    - использовать в ограничении по исх.деб.сал.перерасчеты?
     * @param isUseCorrPayInRestrict   - использовать в ограничении по исх.деб.сал.корр оплаты?
     * @param isUsePayInRestrict       - использовать в ограничении по исх.деб.сал.оплату?
     * @param isIncludeByClosedOrgList - включая услуги и организации по списку закрытых организаций?
     * @param isExcludeByClosedOrgList - исключая услуги и организации по списку закрытых организаций?
     * @param lstExcludeUslId          - список Id услуг, которые исключить из базовой коллекции для распределения
     */
    private void distWithRestriction(Amount amount, int tp, boolean isRestrictByOutSal,
                                     Boolean isUseChargeInRestrict, Boolean isUseChangeInRestrict,
                                     Boolean isUseCorrPayInRestrict, Boolean isUsePayInRestrict,
                                     boolean isIncludeByClosedOrgList,
                                     boolean isExcludeByClosedOrgList, List<String> lstExcludeUslId) throws WrongParam {
        if (!isRestrictByOutSal && isUseChargeInRestrict != null && isUseChangeInRestrict != null
                && isUseCorrPayInRestrict != null && isUsePayInRestrict != null) {
            throw new WrongParam("Некорректно заполнять isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=false!");
        } else if (isRestrictByOutSal && (isUseChargeInRestrict == null && isUseChangeInRestrict == null
                && isUseCorrPayInRestrict == null && isUsePayInRestrict == null)) {
            throw new WrongParam("Незаполнено isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=true!");
        }

        // Распределить на все услуги
        String currPeriod = configApp.getPeriod();
        List<SumUslOrgDTO> lstDistribBase;
        // получить базовую коллекцию для распределения
        if (tp == 0) {
            // получить вх.сал.
            lstDistribBase = amount.getInSal();
        /*saldoMng.getOutSal(amount.getKart(), currPeriod, amount.getLstDistPayment(), null,
                    true, false, false, false, false);*/

        } else if (tp == 1) {
            // получить начисление+перерасчет-оплата
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, true, false, false, false, false, null);
            // note фильтровать по положительным значениям????
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp == 3) {
            // получить начисление предыдущего периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), configApp.getPeriodBack(),
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, true, false, false, false, false, null);
            // note фильтровать по положительным значениям????
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else {
            throw new WrongParam("Некорректный параметр tp=" + tp);
        }
        // исключить услуги
        if (lstExcludeUslId != null) {
            lstDistribBase.removeIf(t -> lstExcludeUslId.contains(t.getUslId()));
        }
        if (isIncludeByClosedOrgList) {
            // оставить только услуги и организации, содержащиеся в списке закрытых орг.
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().noneMatch(d -> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())  // организация - поставщик
                            && Utl.between2(amount.getKwtpMg().getDopl(), d.getMgFrom(), d.getMgTo()) // период
                    )
            );
        } else if (isExcludeByClosedOrgList) {
            // оставить только услуги и организации, НЕ содержащиеся в списке закрытых орг.
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().anyMatch(d -> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())  // организация - поставщик
                            && Utl.between2(amount.getKwtpMg().getDopl(), d.getMgFrom(), d.getMgTo()) // период
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
        if (tp == 1) {
            saveKwtpDayLog(amount.getKwtpMg(),"Текущее начисление > 0, по лиц.счету lsk={}:",
                    amount.getKart().getLsk());
        }
        saveKwtpDayLog(amount.getKwtpMg(),"Будет распределено по строкам:");
        lstDistribBase.forEach(t -> saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
        saveKwtpDayLog(amount.getKwtpMg(),"итого:{}", amntSal);

        // распределить сумму по базе распределения
        Map<DistributableBigDecimal, BigDecimal> mapDistPay =
                Utl.distBigDecimalByListIntoMap(amount.getSumma(), lstDistribBase, 2);

        // распечатать предварительное распределение оплаты
        BigDecimal distSumma = saveDistPay(amount, mapDistPay, !isRestrictByOutSal);

        if (isRestrictByOutSal) {
            saveKwtpDayLog(amount.getKwtpMg(),"Сумма для распределения будет ограничена по исх.сальдо");
            if (distSumma.compareTo(BigDecimal.ZERO) != 0) {
                // Ограничить суммы  распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                        amount.getLstDistPayment(), amount.getLstDistControl(),
                        true, isUseChargeInRestrict, false, isUseChangeInRestrict, isUseCorrPayInRestrict,
                        isUsePayInRestrict, null);
                saveKwtpDayLog(amount.getKwtpMg(),"Исх.сальдо по лиц.счету lsk={}:",
                        amount.getKart().getLsk());
                lstOutSal.forEach(t -> saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));

                // ограничить суммы распределения по исх.сал.
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

                                    saveKwtpDayLog(amount.getKwtpMg(),"распределение ограничено по исх.сал. usl={}, org={}, summa={}",
                                            sal.getUslId(), sal.getOrgId(), dist.getValue());
                                });
                    }
                }

                // сохранить, распечатать распределение оплаты
                distSumma = saveDistPay(amount, mapDistPay, true);

                amount.setSumma(amount.getSumma().add(distSumma.negate()));
                saveKwtpDayLog(amount.getKwtpMg(),"итого распределено:{}, остаток:{}", distSumma, amount.getSumma());


                lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod, amount.getLstDistPayment(), amount.getLstDistPayment(),
                        true, true, false, true, true, true, null);
                saveKwtpDayLog(amount.getKwtpMg(),"После распределения оплаты: Исх.сальдо по лиц.счету lsk={}:",
                        amount.getKart().getLsk());
                lstOutSal.forEach(t -> saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));

            }
        }
    }

    /**
     * Распределить платеж экслюзивно, на одну услугу
     *
     * @param amount           - итоги
     * @param includeOnlyUslId - список Id услуг, на которые распределить оплату, не зависимо от их сальдо
     */
    private void distExclusivelyBySingleUslId(Amount amount,
                                              @SuppressWarnings("SameParameterValue") String includeOnlyUslId) throws ErrorWhileDistPay {
        // Распределить эксклюзивно на одну услугу
        Nabor nabor = naborDAO.getByLskUsl(amount.getKart().getLsk(), includeOnlyUslId);
        if (nabor != null) {
            // сохранить для записи в KWTP_DAY
            BigDecimal distSumma = amount.getSumma();
            amount.getLstDistPayment().add(
                    new SumUslOrgDTO(includeOnlyUslId, nabor.getOrg().getId(), distSumma));
            amount.setSumma(amount.getSumma().add(distSumma.negate()));
            saveKwtpDayLog(amount.getKwtpMg(),"Распределено фактически:");
            saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}", includeOnlyUslId, nabor.getOrg().getId(), distSumma);
            saveKwtpDayLog(amount.getKwtpMg(),"итого распределено:{}, остаток:{}", distSumma, amount.getSumma());
        } else {
            throw new ErrorWhileDistPay("При распределении не найдена запись в наборе услуг lsk="
                    + amount.getKart().getLsk()
                    + "usl=" + includeOnlyUslId);
        }
    }

    /**
     * Сохранить, суммировать и распечатать распределение
     *
     * @param amount     - Итоги
     * @param mapDistPay - коллекция распределения
     * @param isSave     - сохранить распределение?
     */
    private BigDecimal saveDistPay(Amount amount, Map<DistributableBigDecimal, BigDecimal> mapDistPay,
                                   boolean isSave) {
        if (isSave) {
            saveKwtpDayLog(amount.getKwtpMg(),"Распределено фактически:");
        } else {
            saveKwtpDayLog(amount.getKwtpMg(),"Распределено предварительно:");
        }
        BigDecimal amnt = BigDecimal.ZERO;
        for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay.entrySet()) {
            SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) t.getKey();
            if (isSave) {
                // сохранить для записи в KWTP_DAY
                amount.getLstDistPayment().add(
                        new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
            } else {
                // сохранить для контроля
                amount.getLstDistControl().add(
                        new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
            }
            amnt = amnt.add(t.getValue());
            saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                    sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
        }

        saveKwtpDayLog(amount.getKwtpMg(),"итого распределено:{}", amnt);
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
     * @param kwtpMg - строка оплаты по периоду
     * @param text - сообщение
     * @param t - параметры
     */
    private void saveKwtpDayLog(KwtpMg kwtpMg, String text, Object... t) {
        KwtpDayLog kwtpDayLog =
                KwtpDayLog.KwtpDayLogBuilder.aKwtpDayLog()
                        .withFkKwtpMg(kwtpMg.getId())
                        .withText(text).build();
/*
        KwtpDayLog kwtpDayLog =
                KwtpDayLog.KwtpMgLogBuilder.aKwtpMgLog()
                        .withKwtpMg(kwtpMg).withText(text).build();
*/
        //kwtpMg.getKwtpMgLog().add(kwtpDayLog);
        em.persist(kwtpDayLog);
        log.info(text, t);
    }
}
