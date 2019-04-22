package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.app.mm.GenChrgProcessMng;
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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

/**
 * Сервис распределения оплаты
 */
@Slf4j
@Service
public class DistPayMngImpl implements DistPayMng {

    private final GenChrgProcessMng genChrgProcessMng;
    private final SaldoMng saldoMng;
    private final ConfigApp configApp;
    private final SprProcPayDAO sprProcPayDAO;
    private final NaborDAO naborDAO;
    private final SaldoUslDAO saldoUslDAO;

    @PersistenceContext
    private EntityManager em;

    public DistPayMngImpl(SaldoMng saldoMng, ConfigApp configApp,
                          SprProcPayDAO sprProcPayDAO, NaborDAO naborDAO, SaldoUslDAO saldoUslDAO,
                          GenChrgProcessMng genChrgProcessMng) {
        this.saldoMng = saldoMng;
        this.configApp = configApp;
        this.sprProcPayDAO = sprProcPayDAO;
        this.naborDAO = naborDAO;
        this.saldoUslDAO = saldoUslDAO;
        this.genChrgProcessMng = genChrgProcessMng;
    }

    /**
     * Класс итогов распределения
     */
    @Getter
    @Setter
    private class Amount {
        // Cтрока платежа:
        // лиц.счет
        private Kart kart;
        // id строки платежа по периодам
        int kwtpMgId;
        // сумма оплаты
        private BigDecimal summa = BigDecimal.ZERO;
        // сумма оплаченной пени
        private BigDecimal penya = BigDecimal.ZERO;
        // период платежа
        String dopl;
        // инкассация
        int nink;
        // № комп.
        String nkom;
        // код операции
        String oper;
        // дата платежа
        Date dtek;
        // дата инкассации
        Date datInk;

        // Прочие параметры:
        // входящее, общее сальдо
        private List<SumUslOrgDTO> inSal;
        // итог по общ.сал.
        private BigDecimal amntInSal = BigDecimal.ZERO;
        // итог по начислению предыдущего периода
        private BigDecimal amntChrgPrevPeriod = BigDecimal.ZERO;

        // список закрытых орг.
        List<SprProcPay> lstSprProcPay;
        // уже распределенная оплата
        private List<SumUslOrgDTO> lstDistPayment = new ArrayList<>();
        // уже распределенная пеня
        private List<SumUslOrgDTO> lstDistPenya = new ArrayList<>();
        // уже распределенная оплата - для контроля
        private List<SumUslOrgDTO> lstDistControl = new ArrayList<>();
    }

    /**
     * Распределить платеж (запись в C_KWTP_MG)
     * @param kwtpMgId - ID записи C_KWTP_MG
     * @param lsk       - лиц.счет
     * @param strSumma  - сумма оплаты долга
     * @param strPenya  - сумма оплаты пени
     * @param dopl      - период оплаты
     * @param nink      - № инкассации
     * @param nkom      - № компьютера
     * @param oper      - код операции
     * @param strDtek   - дата платежа
     * @param strDtInk - дата инкассации
     * @param isTest - тестирование? (не будет вызвано начисление)
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class) //
    public void distKwtpMg(int kwtpMgId,
                           String lsk,
                           String strSumma,
                           String strPenya,
                           String dopl,
                           int nink,
                           String nkom,
                           String oper,
                           String strDtek,
                           String strDtInk,
                           boolean isTest) throws ErrorWhileDistPay {

        saveKwtpDayLog(kwtpMgId, "***** Распределение оплаты *****");
        try {
            Amount amount = buildAmount(kwtpMgId,
                    !isTest,
                    lsk,
                    strSumma,
                    strPenya,
                    dopl,
                    nink,
                    nkom,
                    oper,
                    strDtek,
                    strDtInk);
            Org uk = amount.getKart().getUk();
            // общий способ распределения
            if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                saveKwtpDayLog(amount.getKwtpMgId(), "1.0 Сумма оплаты долга > 0");

                if (amount.getSumma().compareTo(amount.amntInSal) == 0) {
                    saveKwtpDayLog(amount.getKwtpMgId(), "2.0 Сумма оплаты = вх.деб.+кред.");
                    saveKwtpDayLog(amount.getKwtpMgId(), "2.1 Распределить по вх.деб.+кред. по списку закрытых орг, c ограничением по исх.сал.");
                    distWithRestriction(amount, 0, true, true,
                            true, true, true,
                            true, false, null, true);

                    saveKwtpDayLog(amount.getKwtpMgId(), "2.2 Распределить по вх.деб.+кред. остальные услуги, кроме списка закрытых орг. " +
                            "без ограничения по исх.сальдо");
                    distWithRestriction(amount, 0, false, null,
                            null, null, null,
                            false, true, null, true);
                } else if (amount.getSumma().compareTo(amount.amntInSal) > 0) {
                    saveKwtpDayLog(amount.getKwtpMgId(), "3.0 Сумма оплаты > вх.деб.+кред. (переплата)");
                    boolean flag = false;
                    if (uk.getDistPayTp().equals(0)) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMgId(), "3.1.1 Тип распределения - общий");
                    } else if (amount.getAmntInSal().compareTo(amount.getAmntChrgPrevPeriod()) > 0) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMgId(), "3.1.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. > долг.1 мес.");
                    }
                    if (flag) {
                        saveKwtpDayLog(amount.getKwtpMgId(), "3.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                true, false, null, true);

                        saveKwtpDayLog(amount.getKwtpMgId(), "3.1.3 Распределить по начислению предыдущего периода={}, без ограничения по исх.сал.",
                                configApp.getPeriodBack());
                        distWithRestriction(amount, 3, false, null,
                                null, null, null,
                                false, false, null, true);
                    } else {
                        saveKwtpDayLog(amount.getKwtpMgId(), "3.2.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. <= долг.1 мес.");

                        saveKwtpDayLog(amount.getKwtpMgId(), "3.2.2 Распределить оплату по вх.деб.+кред. без услуги 003, c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true,
                                true, false,
                                false, Collections.singletonList("003"), true);
                        if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                            saveKwtpDayLog(amount.getKwtpMgId(), "3.2.3 Остаток распределить на услугу usl=003 ");
                            distExclusivelyBySingleUslId(amount, "003", true);
                        }
                    }
                } else {
                    saveKwtpDayLog(amount.getKwtpMgId(), "4.0 Сумма оплаты < долг (недоплата)");
                    final BigDecimal rangeBegin = new BigDecimal("0.01");
                    final BigDecimal rangeEnd = new BigDecimal("100");
                    boolean flag = false;
                    if (uk.getDistPayTp().equals(0)) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMgId(), "4.1.1 Тип распределения - общий");
                    } else if (amount.getAmntInSal().subtract(amount.getSumma()).compareTo(rangeEnd) > 0) {
                        flag = true;
                        saveKwtpDayLog(amount.getKwtpMgId(), "4.1.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                "> 100");
                    }
                    if (flag) {
                        saveKwtpDayLog(amount.getKwtpMgId(), "4.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                true, false, null, true);

                        saveKwtpDayLog(amount.getKwtpMgId(), "4.1.3 Распределить по вх.деб.+кред. остальные услуги, кроме списка закрытых орг. " +
                                "без ограничения по исх.сальдо");
                        distWithRestriction(amount, 0, false, null,
                                null, null, null,
                                false, true, null, true);
                    } else {
                        saveKwtpDayLog(amount.getKwtpMgId(), "4.2.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                "<= 100");
                        saveKwtpDayLog(amount.getKwtpMgId(), "4.2.2 Распределить оплату по вх.деб.+кред. без услуги 003, по списку закрытых орг, " +
                                "c ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true,
                                true, true,
                                true, true,
                                false, Collections.singletonList("003"), true);

                        saveKwtpDayLog(amount.getKwtpMgId(), "4.2.3 Распределить по вх.деб.+кред. остальные услуги, кроме списка закрытых орг. " +
                                "с ограничением по исх.сальдо");
                        distWithRestriction(amount, 0, true, true,
                                true, true, true,
                                false, true, null, true);
                        if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                            saveKwtpDayLog(amount.getKwtpMgId(), "4.2.4 Остаток распределить на услугу usl=003");
                            distExclusivelyBySingleUslId(amount, "003", true);
                        }
                    }
                }
            } else {
                saveKwtpDayLog(amount.getKwtpMgId(), "2.0 Сумма оплаты < 0, снятие ранее принятой оплаты");
                // сумма оплаты < 0 (снятие оплаты)
                // note - проверить!!! ????? должно выполняться в отдельной процедуре? (уже есть такое в PL/SQL, написано нормально
            }

            if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                // распределение пени
                saveKwtpDayLog(amount.getKwtpMgId(), "5.0 Сумма пени > 0");
                saveKwtpDayLog(amount.getKwtpMgId(), "5.0.1 Распределить по уже имеющемуся распределению оплаты");
                distWithRestriction(amount, 4, false, null,
                        null, null, null,
                        false, false, null, false);
                if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                    saveKwtpDayLog(amount.getKwtpMgId(), "5.0.2 Остаток распределить по начислению");
                    distWithRestriction(amount, 5, false, null,
                            null, null, null,
                            false, false, null, false);
                    if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                        saveKwtpDayLog(amount.getKwtpMgId(), "5.0.3 Не распределено по начислению, " +
                                "распределить на услугу usl=003");
                        distExclusivelyBySingleUslId(amount, "003", false);

                    }
                }

            } else {

            }

            // сгруппировать распределение оплаты
            List<SumUslOrgDTO> lstForKwtpDayPay = new ArrayList<>(20);
            amount.getLstDistPayment().forEach(t ->
                    saldoMng.groupByUslOrg(lstForKwtpDayPay, t));
            // сгруппировать распределение пени
            List<SumUslOrgDTO> lstForKwtpDayPenya = new ArrayList<>(20);
            amount.getLstDistPenya().forEach(t ->
                    saldoMng.groupByUslOrg(lstForKwtpDayPenya, t));
            Map<Integer, List<SumUslOrgDTO>> mapForKwtpDay = new HashMap<>();
            mapForKwtpDay.put(1, lstForKwtpDayPay);
            mapForKwtpDay.put(0, lstForKwtpDayPenya);

            // сохранить распределение в KWTP_DAY
            saveKwtpDayLog(amount.getKwtpMgId(), "5.0 Итоговое, сгруппированное распределение в KWTP_DAY:");

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
                saveKwtpDayLog(amount.getKwtpMgId(), "tp={}, usl={}, org={}, summa={}",
                        kwtpDay.getTp(), kwtpDay.getUsl().getId(), kwtpDay.getOrg().getId(), d.getSumma());
                em.persist(kwtpDay);
            }));

        } catch (WrongParam wrongParam) {
            throw new ErrorWhileDistPay("Произошла ошибка в процессе распределения оплаты!");
        }
    }

    /**
     * Построить объект расчета
     *
     */
    private Amount buildAmount(int kwtpMgId, boolean isGenChrg, String lsk, String summaStr, String penyaStr, String dopl,
                               int nink, String nkom, String oper, String dtekStr, String datInkStr) throws WrongParam {
        Amount amount = new Amount();
        Kart kart = em.find(Kart.class, lsk);
        amount.setKart(kart);
        amount.setSumma(summaStr!=null?new BigDecimal(summaStr):BigDecimal.ZERO);
        amount.setPenya(penyaStr!=null?new BigDecimal(penyaStr):BigDecimal.ZERO);
        amount.setKwtpMgId(kwtpMgId);
        amount.setDopl(dopl);
        amount.setNink(nink);
        amount.setNkom(nkom);
        amount.setOper(oper);
        amount.setDtek(Utl.getDateFromStr(dtekStr));
        amount.setDatInk(datInkStr!=null? Utl.getDateFromStr(datInkStr): null);

        saveKwtpDayLog(amount.getKwtpMgId(), "1.0 C_KWTP_MG.ID={}, C_KWTP_MG.SUMMA={}, C_KWTP_MG.PENYA={}",
                amount.getKwtpMgId(), amount.getSumma(), amount.getPenya());
        saveKwtpDayLog(amount.getKwtpMgId(), "    KART.REU={}", amount.getKart().getUk().getReu());

        if (isGenChrg) {
            // сформировать начисление
            genChrgProcessMng.genChrg(0, 0, amount.getDtek(), null, null,
                    amount.getKart().getKoKw(), null, null);
        }
        // получить вх.общ.сал.
        amount.setInSal(saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                null, null,
                true, false, false, false, false, false, null));
        // итог по вх.сал.
        amount.setAmntInSal(amount.getInSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        saveKwtpDayLog(amount.getKwtpMgId(), "Итого вх.сал.={}", amount.getAmntInSal());
        // получить начисление за прошлый период
        List<SumUslOrgDTO> lstChrgPrevPeriod =
                saldoMng.getOutSal(amount.getKart(), configApp.getPeriodBack(),
                        null, null,
                        false, true, false, true, true, true, null);
        // получить итог начисления за прошлый период
        amount.setAmntChrgPrevPeriod(lstChrgPrevPeriod
                .stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        saveKwtpDayLog(amount.getKwtpMgId(), "Итого сумма нач.за прошлый период={}", amount.getAmntChrgPrevPeriod());

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
     * @param tp                       - тип 0-по вх.деб.сал.+кред.сал, 1- по начислению+перерасчет-оплата,
     *                                 2- по деб.сал-оплата, 3 -по начислению заданного периода
     *                                 4- по уже готовому распределению оплаты долга (распр.пени обычно)
     *                                 5- по начислению текущего периода
     * @param isRestrictByOutSal       - ограничить по исх.сал. (проверять чтобы не создавалось кред.сальдо)?
     * @param isUseChargeInRestrict    - использовать в ограничении по исх.деб.сал.начисление?
     * @param isUseChangeInRestrict    - использовать в ограничении по исх.деб.сал.перерасчеты?
     * @param isUseCorrPayInRestrict   - использовать в ограничении по исх.деб.сал.корр оплаты?
     * @param isUsePayInRestrict       - использовать в ограничении по исх.деб.сал.оплату?
     * @param isIncludeByClosedOrgList - включая услуги и организации по списку закрытых организаций?
     * @param isExcludeByClosedOrgList - исключая услуги и организации по списку закрытых организаций?
     * @param lstExcludeUslId          - список Id услуг, которые исключить из базовой коллекции для распределения
     * @param isDistPay                - распределять оплату - да, пеню - нет
     */
    private void distWithRestriction(Amount amount, int tp, boolean isRestrictByOutSal,
                                     Boolean isUseChargeInRestrict, Boolean isUseChangeInRestrict,
                                     Boolean isUseCorrPayInRestrict, Boolean isUsePayInRestrict,
                                     boolean isIncludeByClosedOrgList,
                                     boolean isExcludeByClosedOrgList, List<String> lstExcludeUslId,
                                     boolean isDistPay) throws WrongParam {
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
        } else if (tp == 1) {
            // получить начисление+перерасчет-оплата
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, true, false, false, false, false, null);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp == 3) {
            // получить начисление предыдущего периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, true, false, false, false, false,
                    configApp.getPeriodBack());
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
                    false, true, false, false, false, false, null);
            // фильтровать по положительным значениям
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
        if (tp == 1) {
            saveKwtpDayLog(amount.getKwtpMgId(), "Текущее начисление > 0, по лиц.счету lsk={}:",
                    amount.getKart().getLsk());
        }
        saveKwtpDayLog(amount.getKwtpMgId(), "Будет распределено по строкам:");
        lstDistribBase.forEach(t ->
                saveKwtpDayLog(amount.getKwtpMgId(), "usl={}, org={}, summa={}",
                    t.getUslId(), t.getOrgId(), t.getSumma()));
        saveKwtpDayLog(amount.getKwtpMgId(), "итого:{}", amntSal);

        // распределить сумму по базе распределения
        Map<DistributableBigDecimal, BigDecimal> mapDistPay;
        if (isDistPay) {
            // распределить оплату
            mapDistPay =
                    Utl.distBigDecimalByListIntoMap(amount.getSumma(), lstDistribBase, 2);
        } else {
            // распределить пеню
            mapDistPay =
                    Utl.distBigDecimalByListIntoMap(amount.getPenya(), lstDistribBase, 2);
        }
        // распечатать предварительное распределение оплаты или сохранить, если не будет ограничения по сальдо
        BigDecimal distSumma = saveDistPay(amount, mapDistPay, !isRestrictByOutSal, isDistPay);

        if (isRestrictByOutSal) {
            saveKwtpDayLog(amount.getKwtpMgId(), "Сумма для распределения будет ограничена по исх.сальдо");
            if (distSumma.compareTo(BigDecimal.ZERO) != 0) {
                // Ограничить суммы  распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                        amount.getLstDistPayment(), amount.getLstDistControl(),
                        true, isUseChargeInRestrict, false, isUseChangeInRestrict, isUseCorrPayInRestrict,
                        isUsePayInRestrict, null);
                saveKwtpDayLog(amount.getKwtpMgId(), "Исх.сальдо по лиц.счету lsk={}:",
                        amount.getKart().getLsk());
                lstOutSal.forEach(t -> saveKwtpDayLog(amount.getKwtpMgId(), "usl={}, org={}, summa={}",
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

                                    saveKwtpDayLog(amount.getKwtpMgId(),
                                            "распределение ограничено по исх.сал. usl={}, org={}, summa={}",
                                            sal.getUslId(), sal.getOrgId(), dist.getValue());
                                });
                    }
                }

                // сохранить, распечатать распределение оплаты
                distSumma = saveDistPay(amount, mapDistPay, true, isDistPay);
                lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                        amount.getLstDistPayment(), amount.getLstDistPayment(),
                        true, true, false, true,
                        true, true, null);
                if (isDistPay) {
                    saveKwtpDayLog(amount.getKwtpMgId(), "После распределения оплаты: Исх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                }
                lstOutSal.forEach(t -> saveKwtpDayLog(amount.getKwtpMgId(), "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
            }
        }

        // вычесть из итоговой суммы платежа
        if (isDistPay) {
            amount.setSumma(amount.getSumma().add(distSumma.negate()));
            saveKwtpDayLog(amount.getKwtpMgId(), "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        } else {
            amount.setPenya(amount.getPenya().add(distSumma.negate()));
            saveKwtpDayLog(amount.getKwtpMgId(), "итого распределено:{}, остаток:{}",
                    distSumma, amount.getPenya());
        }



    }

    /**
     * Распределить платеж экслюзивно, на одну услугу
     *  @param amount           - итоги
     * @param includeOnlyUslId - список Id услуг, на которые распределить оплату, не зависимо от их сальдо
     * @param isDistPay                - распределять оплату - да, пеню - нет
     */
    private void distExclusivelyBySingleUslId(Amount amount,
                                              @SuppressWarnings("SameParameterValue") String includeOnlyUslId, boolean isDistPay) throws ErrorWhileDistPay {
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
            saveKwtpDayLog(amount.getKwtpMgId(), "Распределено фактически:");
            saveKwtpDayLog(amount.getKwtpMgId(), "usl={}, org={}, summa={}",
                    includeOnlyUslId, nabor.getOrg().getId(), distSumma);
            saveKwtpDayLog(amount.getKwtpMgId(), "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        } else {
            throw new ErrorWhileDistPay("При распределении не найдена запись в наборе услуг lsk="
                    + amount.getKart().getLsk()
                    + "usl=" + includeOnlyUslId);
        }
    }

    /**
     * Сохранить, суммировать и распечатать распределение
     *  @param amount     - Итоги
     * @param mapDistPay - коллекция распределения
     * @param isSave     - сохранить распределение?
     * @param isDistPay - распределить оплату - да, пеню - нет
     */
    private BigDecimal saveDistPay(Amount amount, Map<DistributableBigDecimal, BigDecimal> mapDistPay,
                                   boolean isSave, boolean isDistPay) {
        String msg = "оплаты";
        if (!isDistPay) {
            msg = "пени";
        }
        if (isSave) {
            saveKwtpDayLog(amount.getKwtpMgId(), "Распределено {}, фактически:", msg);
        } else {
            saveKwtpDayLog(amount.getKwtpMgId(), "Распределено {}, предварительно:", msg);
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
            saveKwtpDayLog(amount.getKwtpMgId(), "usl={}, org={}, summa={}",
                    sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
        }

        saveKwtpDayLog(amount.getKwtpMgId(), "итого {}, распределено:{}", msg, amnt);
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
     * @param kwtpMgId - строка оплаты по периоду
     * @param msg     - сообщение
     * @param t        - параметры
     */
    private void saveKwtpDayLog(Integer kwtpMgId, String msg, Object... t) {
        KwtpDayLog kwtpDayLog =
                KwtpDayLog.KwtpDayLogBuilder.aKwtpDayLog()
                        .withFkKwtpMg(kwtpMgId)
                        .withText(Utl.getStrUsingTemplate(msg, t)).build();
        em.persist(kwtpDayLog);
        log.info(msg, t);
    }
}
