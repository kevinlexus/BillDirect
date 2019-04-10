package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.KwtpMg;
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Сервис распределения оплаты
 */
@Slf4j
@Service
public class DistPayMngImpl implements DistPayMng {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private SaldoMng saldoMng;
    @Autowired
    private ConfigApp configApp;

    /**
     * Распределить платеж (запись в C_KWTP_MG)
     */
    @Override
    public void distKwtpMg(KwtpMg kwtpMg) throws ErrorWhileDistPay {
        log.info("***** Распределение оплаты *****");
        BigDecimal summa = Utl.nvl(kwtpMg.getSumma(), BigDecimal.ZERO);
        BigDecimal penya = Utl.nvl(kwtpMg.getPenya(), BigDecimal.ZERO);

        log.info("1.0 C_KWTP_MG.ID={}, C_KWTP_MG.SUMMA={}, C_KWTP_MG.PENYA={}",
                kwtpMg.getId(), summa, penya);
        Kart kart = kwtpMg.getKart();
        String currPeriod = configApp.getPeriod();
        if (summa.compareTo(BigDecimal.ZERO) > 0) {
            // сумма оплаты > 0
            // Получить входящее сальдо
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(kart, currPeriod,
                    true, false, false, false, false);
            // сумма дебетового сальдо
            List<SumUslOrgDTO> lstInDebSal = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0).collect(Collectors.toList());
            BigDecimal amntSal = lstInDebSal.stream()
                    .map(SumUslOrgDTO::getSumma)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            log.info("2.0 Вх.деб.сальдо по лиц.счету lsk={}:",
                    kwtpMg.getKart().getLsk());
            lstInDebSal.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
            log.info("итого вх.деб.:{}", amntSal);

            // Ограничить сумму для распределения
            BigDecimal sumForDist;
            if (summa.compareTo(amntSal) > 0) {
                sumForDist = amntSal;
                log.info("2.1 Сумма для распределения ограничена по деб.сальдо:{}", sumForDist);
            } else {
                sumForDist = summa;
            }

            // Распределить сумму по вх.деб.сальдо
            Map<DistributableBigDecimal, BigDecimal> mapDistPay =
                    Utl.distBigDecimalByListIntoMap(sumForDist, lstInDebSal, 2);
            log.info("2.2 Распределено по вх.деб.:");
            BigDecimal amnt = BigDecimal.ZERO;
            for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay.entrySet()) {
                SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) t.getKey();
                amnt = amnt.add(t.getValue());
                log.info("usl={}, org={}, summa={}", sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
            }
            log.info("итого распределено:{}", amnt);

            if (amnt.compareTo(BigDecimal.ZERO) > 0) {
                // Ограничить суммы распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> outSal = saldoMng.getOutSal(kart, currPeriod,
                        true, true, true, true, true);
                List<SumUslOrgDTO> lstOutDebSal = outSal.stream()
                        .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0).collect(Collectors.toList());

                log.info("2.3 Исх.деб.сальдо по лиц.счету lsk={}:",
                        kwtpMg.getKart().getLsk());
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
                            ((SumUslOrgDTO) t).getUslId(), ((SumUslOrgDTO) t).getOrgId(), 0);

                });

                amnt = mapDistPay.values().stream().reduce(BigDecimal.ZERO, BigDecimal::add);
                log.info("итого распределено:{}", amnt);
                summa = summa.add(amnt.negate());
                if (summa.compareTo(BigDecimal.ZERO) > 0) {
                    // Распределить переплату
                    log.info("3.0 Переплата:{}", summa);

                    // 1. Вар. - распределить по текущему начислению
                    // Получить начисление
                    List<SumUslOrgDTO> curChrg = saldoMng.getOutSal(kart, currPeriod,
                            false, true, false, false, false);
                    // фильтровать по положительным значениям (вдруг начисление отрицательное по минусовым показаниям счетчиков)
                    List<SumUslOrgDTO> lstCurChrg = curChrg.stream()
                            .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0).collect(Collectors.toList());
                    log.info("3.1 Текущее начисление > 0, по лиц.счету lsk={}:",
                            kwtpMg.getKart().getLsk());
                    lstCurChrg.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));

                    // распределить по начислению
                    Map<DistributableBigDecimal, BigDecimal> mapDistPay2 =
                            Utl.distBigDecimalByListIntoMap(summa, lstCurChrg, 2);
                    log.info("3.2 Распределено по тек.нач.:");
                    amnt = BigDecimal.ZERO;
                    for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay2.entrySet()) {
                        SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) t.getKey();
                        amnt = amnt.add(t.getValue());
                        log.info("usl={}, org={}, summa={}", sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
                    }
                    log.info("итого распределено:{}", amnt);

                    // 2. Вар. - УК 14, 15 - распределить по другому алгоритму

                } else if (summa.compareTo(BigDecimal.ZERO) < 0) {
                    throw new ErrorWhileDistPay("ОШИБКА! Отрицательная сумма после распределения оплаты!");
                }

            }
        }
    }
}
