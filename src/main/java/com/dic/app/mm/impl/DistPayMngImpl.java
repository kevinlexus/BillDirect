package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.bill.dao.SprProcPayDAO;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.KwtpMg;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.SprProcPay;
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Сервис распределения оплаты
 */
@Slf4j
@Service
public class DistPayMngImpl implements DistPayMng {

    private final SaldoMng saldoMng;
    private final ConfigApp configApp;
    private final SprProcPayDAO sprProcPayDAO;

    public DistPayMngImpl(SaldoMng saldoMng, ConfigApp configApp,
                          SprProcPayDAO sprProcPayDAO) {
        this.saldoMng = saldoMng;
        this.configApp = configApp;
        this.sprProcPayDAO = sprProcPayDAO;
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
        // входящее, дебетовое сальдо
        private List<SumUslOrgDTO> inDebSal;
        // итог по вх.деб.сал.
        private BigDecimal amntInDebSal = BigDecimal.ZERO;
        // список закрытых орг.
        List<SprProcPay> lstSprProcPay;
    }

    /**
     * Распределить платеж (запись в C_KWTP_MG)
     */
    @Override
    public void distKwtpMg(KwtpMg kwtpMg) throws ErrorWhileDistPay {
        log.info("***** Распределение оплаты *****");
        Amount amount = buildAmount(kwtpMg);
        Org uk = amount.getKart().getUk();

        log.info("1.0 C_KWTP_MG.ID={}, C_KWTP_MG.SUMMA={}, C_KWTP_MG.PENYA={}",
                kwtpMg.getId(), amount.getSumma(), amount.getPenya());
        // общий способ распределения
        if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
            log.info("2.0 Сумма оплаты > 0");
            if (uk.getDistPayTp().equals(0)) {
                log.info("3.0 Тип распределения - общий");
                // сумма оплаты > 0, распределить по деб.сал, с ограничением
                log.info("3.1 Распределить по вх.деб.сал. c ограничением по исх.сал.");
                distWithRestriction(amount, 0, true, true, null);
                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                    // Распределить переплату по начислению, без ограничения
                    log.info("3.2 Распределить переплату по начислению, без ограничения по исх.сал.");
                    distWithRestriction(amount, 1, false, true, null);

                } else if (amount.getSumma().compareTo(BigDecimal.ZERO) < 0) {
                    throw new ErrorWhileDistPay("ОШИБКА! Отрицательная сумма после распределения оплаты!");
                }
            } else {
                // УК 14,15 - другой способ распределения
                log.info("4.0 Тип распределения - сложный (Для УК 14,15)");
                BigDecimal overPay = amount.getSumma().subtract(amount.getAmntInDebSal());
                if (Utl.between(overPay, new BigDecimal("0.01"), new BigDecimal("100"))) {
                    log.info("4.1 Переплата в диапазоне от 0.01 до 100 руб. включительно, составила:{}, " +
                            "распределить оплату по вх.деб.сал.", overPay);
                    log.info("    с ограничением по исх.сал., без услуги 003");
                    distWithRestriction(amount, 0, true, true, Collections.singletonList("003"));
                    if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                        log.info("4.1.1 Остаток распределить на вх.деб.сал.закрытых орг. " +
                                "- по списку, с ограничением по исх.сал.");
                        distWithRestriction(amount, 0, true, true, null);
                    }
                } else {
                    log.info("4.2 Переплата > 100 руб. составила:{}", overPay);

                }
            }
        } else {
            log.info("2.0 Сумма оплаты < 0, снятие ранее принятой оплаты");
            // сумма оплаты < 0 (снятие оплаты)
            // TODO
        }
    }

    /**
     * Построить объект расчета
     * @param kwtpMg - строка платежа
     */
    private Amount buildAmount(KwtpMg kwtpMg) {
        Amount amount = new Amount();
        amount.setKwtpMg(kwtpMg);
        amount.setKart(kwtpMg.getKart());
        amount.setSumma(Utl.nvl(kwtpMg.getSumma(), BigDecimal.ZERO));
        amount.setPenya(Utl.nvl(kwtpMg.getPenya(), BigDecimal.ZERO));
        // получить вх.деб.сал.
        amount.setInDebSal(saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                true, false, false, false, false)
                .stream().filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                .collect(Collectors.toList()));
        // итог по вх.деб.сал.
        amount.setAmntInDebSal(amount.getInDebSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        log.info("Вх.деб.сальдо по лиц.счету lsk={}:",
                amount.getKart().getLsk());
        amount.getInDebSal().forEach(t -> log.info("usl={}, org={}, summa={}",
                t.getUslId(), t.getOrgId(), t.getSumma()));
        amount.setLstSprProcPay(sprProcPayDAO.findAll());
        log.info("итого:{}", amount.getAmntInDebSal());
        return amount;
    }

    /**
     * Распределить платеж
     * @param amount           - итоги
     * @param tp               - тип 0-по деб.сальдо, 1- по начислению
     * @param isRestrict       - ограничить по исх.деб.сал.
     * @param isUseRestrictedList - использовать список закрытых организаций
     * @param lstRestrictUslId - список Id услуг, которые исключить из базовой коллекции для распределения
     */
    private void distWithRestriction(Amount amount, int tp, boolean isRestrict, boolean isUseRestrictedList,
                                     List<String> lstRestrictUslId) {
        String currPeriod = configApp.getPeriod();
        List<SumUslOrgDTO> lstDistribBase;
        // Получить базовую коллекцию для распределения
        if (tp == 0) {
            // получить вх.деб.сал
            lstDistribBase = amount.getInDebSal();

        } else {
            // получить начисление
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    false, true, false, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        }
        // исключить услуги
        if (lstRestrictUslId != null) {
            lstDistribBase.removeIf(t -> lstRestrictUslId.contains(t.getUslId()));
        }
        // оставить только услуги и организации, содержащиеся в списке закрытых орг.
        if (isUseRestrictedList) {
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().noneMatch(d-> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())) // организация - поставщик
            );
        }

        BigDecimal amntSal = lstDistribBase.stream()
                .map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        if (tp == 1) {
            log.info("Текущее начисление > 0, по лиц.счету lsk={}:",
                    amount.getKart().getLsk());
        }
        log.info("Будет распределено по строкам:");
        lstDistribBase.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
        log.info("итого:{}", amntSal);

        // Распределить сумму по вх.деб.сальдо
        Map<DistributableBigDecimal, BigDecimal> mapDistPay =
                Utl.distBigDecimalByListIntoMap(amount.getSumma(), lstDistribBase, 2);
        BigDecimal distSumma = sumDistPay(mapDistPay);

        if (isRestrict) {
            log.info("Сумма для распределения будет ограничена по исх.сальдо");
            if (distSumma.compareTo(BigDecimal.ZERO) > 0) {
                // Ограничить суммы распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> outSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                        true, true, true, true, true);
                List<SumUslOrgDTO> lstOutDebSal = outSal.stream()
                        .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0).collect(Collectors.toList());

                log.info("Исх.деб.сальдо по лиц.счету lsk={}:",
                        amount.getKart().getLsk());
                lstOutDebSal.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));


                // ограничить суммы распределения по исх.деб.сал.
                for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay.entrySet()) {
                    SumUslOrgDTO distRec = (SumUslOrgDTO) t.getKey();
                    lstOutDebSal.stream().filter(d -> d.getUslId().equals(distRec.getUslId()) && d.getOrgId().equals(distRec.getOrgId())
                            && d.getSumma().compareTo(t.getValue()) < 0)
                            .forEach(d -> {
                                t.setValue(d.getSumma());
                                log.info("распределение ограничено по исх.деб.сал usl={}, org={}, summa={}",
                                        d.getUslId(), d.getOrgId(), t.getValue());
                            });
                }

                // занулить распределение, по которому нет исх.деб.сал.
                mapDistPay.keySet().stream().map(t -> (SumUslOrgDTO) t)
                        .filter(t -> lstOutDebSal.stream()
                                .noneMatch(d -> d.getUslId().equals(t.getUslId()) &&
                                        d.getOrgId().equals(t.getOrgId()
                                        ))).forEach(t -> {
                    mapDistPay.put(t, BigDecimal.ZERO);
                    log.info("распределение обнулено (нет исх.деб.сал) по usl={}, org={}, summa={}",
                            t.getUslId(), t.getOrgId(), 0);

                });

                distSumma = sumDistPay(mapDistPay);
                amount.setSumma(amount.getSumma().add(distSumma.negate()));
                log.info("итого распределено:{}, остаток:{}", distSumma, amount.getSumma());
            }
        }
    }

    /**
     * Суммировать и распечатать распределение
     *
     * @param mapDistPay - коллекция распределения
     */
    private BigDecimal sumDistPay(Map<DistributableBigDecimal, BigDecimal> mapDistPay) {
        log.info("Распределено:");
        BigDecimal amnt = BigDecimal.ZERO;
        for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay.entrySet()) {
            SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) t.getKey();
            amnt = amnt.add(t.getValue());
            log.info("usl={}, org={}, summa={}", sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
        }
        log.info("итого распределено:{}", amnt);
        return amnt;
    }

}
