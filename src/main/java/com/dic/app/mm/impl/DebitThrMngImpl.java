package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.app.mm.GenPen;
import com.ric.cmn.Utl;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import com.dic.app.mm.DebitThrMng;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис обработки строк задолженности и расчета пени по дням
 *
 * @author lev
 * @version 1.18
 */
@Slf4j
@Service
public class DebitThrMngImpl implements DebitThrMng {

    @PersistenceContext
    private EntityManager em;


    /**
     * Свернуть задолженность, подготовить информацию для расчета пени
     *
     * @param kart       - лиц.счет
     * @param calcStore  - хранилище справочников
     * @param localStore - хранилище всех операций по лиц.счету
     */
    @Override
    public void genDebitUsl(Kart kart, CalcStore calcStore,
                            CalcStoreLocal localStore) {
        // дата начала расчета
        Date dt1 = calcStore.getCurDt1();
        // дата окончания расчета
        Date dt2 = calcStore.getGenDt();

        class DebPeriod {
            private int mg;
            // задолженность
            BigDecimal deb;
            // задолженность для расчета пени
            BigDecimal debForPen;

            DebPeriod(int mg, BigDecimal deb, BigDecimal debForPen) {
                this.mg = mg;
                this.deb = deb;
                this.debForPen = debForPen;
            }
        }

        // загрузить долги предыдущего периода (вх.сальдо)
        Map<UslOrg, DebPeriod> mapDebIn = new HashMap<>();
        localStore.getLstDebFlow()
                .forEach(t -> mapDebIn.put(new UslOrg(t.getUslId(), t.getOrgId()), new DebPeriod(t.getMg(), t.getDebOut(), t.getDebOut())));

        Map<UslOrg, DebPeriod> mapChrg = new HashMap<>();
        // текущее начисление
        localStore.getLstChrgFlow()
                .forEach(t -> mapChrg.put(new UslOrg(t.getUslId(), t.getOrgId()), new DebPeriod(t.getMg(), t.getSumma(), t.getSumma())));

        // перебрать все дни с начала месяца по дату расчета, включительно
        Calendar c = Calendar.getInstance();
        for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();

            // перерасчеты, включая текущий день
            Map<UslOrg, DebPeriod> mapChng = new HashMap<>();
            localStore.getLstChngFlow().stream()
                    .filter(t -> t.getDt().getTime() <= curDt.getTime())
                    .forEach(t -> mapChng.put(new UslOrg(t.getUslId(), t.getOrgId()),
                            new DebPeriod(calcStore.getPeriod(), t.getSumma(), t.getSumma())));

            // вычесть оплату долга, включая текущий день поступления - для обычного долга
            // и не включая для расчета пени
            Map<UslOrg, DebPeriod> mapPayDeb = new HashMap<>();
            localStore.getLstChngFlow().stream()
                    .filter(t -> t.getDt().getTime() <= curDt.getTime())
                    .forEach(t -> mapChng.put(new UslOrg(t.getUslId(), t.getOrgId()),
                            new DebPeriod(calcStore.getPeriod(),
                                    t.getSumma().negate(),
                                    t.getDt().getTime() == curDt.getTime() ? BigDecimal.ZERO : t.getSumma()
                            )));
            // вычесть корректировки оплаты - для расчета долга, включая текущий день
            Map<UslOrg, DebPeriod> mapPayCorr = new HashMap<>();
            localStore.getLstPayCorrFlow().stream()
                    .filter(t -> t.getDt().getTime() <= curDt.getTime())
                    .forEach(t -> mapChng.put(new UslOrg(t.getUslId(), t.getOrgId()),
                            new DebPeriod(calcStore.getPeriod(),
                                    t.getSumma().negate(),
                                    t.getSumma().negate()
                            )));


        }
    }


    /**
     * Расчет задолжности и пени по услуге
     *
     * @param kart      - лицевой счет
     * @param uslOrg    - услуга и организация
     * @param calcStore - хранилище параметров и справочников
     */
/*
    @Override
    public void genDebitUsl(Kart kart, UslOrg uslOrg, CalcStore calcStore,
                            CalcStoreLocal localStore) throws ErrorWhileChrgPen {
        // дата начала расчета
        Date dt1 = calcStore.getCurDt1();
        // дата окончания расчета
        Date dt2 = calcStore.getGenDt();
        Usl usl = null;
        // загрузить услугу
        usl = em.find(Usl.class, uslOrg.getUslId());

        // объект расчета пени
        GenPen genPen = new GenPen(kart, uslOrg, usl, calcStore);

        List<SumDebRec> lstDeb;
        // РАСЧЕТ по дням
        Calendar c = Calendar.getInstance();
        List<SumDebRec> lstDebAllDays = new ArrayList<>(100);
        List<SumDebRec> lstGroupedDebt = new ArrayList<>();

        for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
            Date curDt;
            curDt = c.getTime();
            // является ли текущий день последним расчетным?
            boolean isLastDay = curDt.equals(dt2);

            // ЗАГРУЗИТЬ выбранные финансовые операции на ТЕКУЩУЮ дату расчета curDt
            // долги предыдущего периода (вх.сальдо)
            lstDeb = localStore.getLstDebFlow().stream()
                    .filter(t -> t.getUslId().equals(uslOrg.getUslId()) && t.getOrgId().equals(uslOrg.getOrgId()))
                    .map(t ->
                            new SumDebRec(t.getDebOut(), null, null, null, null,
                                    t.getDebOut(), t.getDebOut(), t.getMg(), t.getTp())
                    )
                    .collect(Collectors.toList());
            // текущее начисление
            lstDeb.addAll(localStore.getLstChrgFlow().stream()
                    .filter(t -> t.getUslId().equals(uslOrg.getUslId()) && t.getOrgId().equals(uslOrg.getOrgId()))
                    .map(t -> new SumDebRec(null, null, null,
                            t.getSumma(), null, t.getSumma(), t.getSumma(), t.getMg(), t.getTp()))
                    .collect(Collectors.toList()));

            // перерасчеты, включая текущий день
            lstDeb.addAll(localStore.getLstChngFlow().stream()
                    .filter(t -> t.getDt().getTime() <= curDt.getTime())
                    .filter(t -> t.getUslId().equals(uslOrg.getUslId()) && t.getOrgId().equals(uslOrg.getOrgId()))
                    .map(t -> new SumDebRec(null, null, null, null, null,
                            t.getSumma(), t.getSumma(), t.getMg(), t.getTp()))
                    .collect(Collectors.toList()));

            // вычесть оплату долга - для расчета долга, включая текущий день (Не включая для задолженности для расчета пени)
            lstDeb.addAll(localStore.getLstPayFlow().stream()
                    .filter(t -> t.getDt().getTime() <= curDt.getTime())
                    .filter(t -> t.getUslId().equals(uslOrg.getUslId()) && t.getOrgId().equals(uslOrg.getOrgId()))
                    .map(t -> new SumDebRec(null, null, t.getSumma(), null, null,
                            (t.getDt().getTime() < dt1.getTime() ? dt1.getTime() : t.getDt().getTime()) //<-- ПРЕДНАМЕРЕННАЯ ошибка
                                    < curDt.getTime() ?  // (Не включая текущий день, для задолжности для расчета пени)
                                    t.getSumma().negate() : BigDecimal.ZERO,
                            t.getDt().getTime() <= curDt.getTime() ? // (включая текущий день, для обычной задолжности)
                                    t.getSumma().negate() : BigDecimal.ZERO,
                            t.getMg(), t.getTp()))
                    .collect(Collectors.toList()));

            // вычесть корректировки оплаты - для расчета долга, включая текущий день
            lstDeb.addAll(localStore.getLstPayCorrFlow().stream()
                    .filter(t -> t.getDt().getTime() <= curDt.getTime())
                    .filter(t -> t.getUslId().equals(uslOrg.getUslId()) && t.getOrgId().equals(uslOrg.getOrgId()))
                    .map(t -> new SumDebRec(null, t.getSumma(), null, null, null,
                            t.getSumma().negate(),
                            t.getSumma().negate(),
                            t.getMg(), t.getTp()))
                    .collect(Collectors.toList()));

            // добавить и сгруппировать все финансовые операции, по состоянию на текущий день
            lstDeb.forEach(t -> addGroupRec(calcStore, lstGroupedDebt, t, isLastDay));
            // свернуть долги (учесть переплаты предыдущих периодов),
            lstDebAllDays.addAll(getRolledDebPen(calcStore, lstGroupedDebt, uslOrg, isLastDay));
        }

        // fixme НЕ правильно! если возвращать, то вернуть долги на последний день по данной usl+org!
        // вернуть все детализированные долги по данной услуге и организации, по дням
        //return lstDebAllDays;
    }
*/


    /**
     * добавление и группировка финансовой операции, для получения сгруппированной задолжности по периоду
     * @param calcStore - хранилище всех операций по лиц.счету
     * @param lstGroupedDebt - сгруппированные долги
     * @param rec - текущая запись долга
     * @param isLastDay - последний день месяца?
     */
/*
    private void addGroupRec(CalcStore calcStore, List<SumDebRec> lstGroupedDebt, SumDebRec rec, Boolean isLastDay) {

        if (rec.getMg() == null) {
            // если mg не заполнено, установить - текущий период (например для начисления)
            rec.setMg(calcStore.getPeriod());
        }
        if (lstGroupedDebt.size() == 0) {
            lstGroupedDebt.add(rec);
        } else {
            // добавить суммы финансовой операции
            SumDebRec foundRec = lstGroupedDebt.stream().filter(t -> t.getMg().equals(rec.getMg()))
                    .findFirst().orElse(null);
            if (foundRec == null) {
                lstGroupedDebt.add(rec);
            } else {
                // для долга для расчета пени
                foundRec.setSumma(foundRec.getSumma().add(rec.getSumma()));
                // для долга как он есть в базе
                foundRec.setDebOut(foundRec.getDebOut().add(rec.getDebOut()));
                // для свернутого долга
                foundRec.setDebRolled(foundRec.getDebRolled().add(rec.getDebRolled()));
                // для отчета
                if (isLastDay) {
                    // вх.сальдо по задолженности
                    foundRec.setDebIn(foundRec.getDebIn().add(rec.getDebIn()));
                    // начисление
                    foundRec.setChrg(foundRec.getChrg().add(rec.getChrg()));
                    // перерасчеты
                    foundRec.setChng(foundRec.getChng().add(rec.getChng()));
                    // оплата задолженности
                    foundRec.setDebPay(foundRec.getDebPay().add(rec.getDebPay()));
                    // корректировки оплаты
                    foundRec.setPayCorr(foundRec.getPayCorr().add(rec.getPayCorr()));
                }
            }
        }
    }

*/

    /**
     * свернуть задолженность (учесть переплаты предыдущих периодов)
     * @param isLastDay - последний ли расчетный день? (для получения итогового долга)
     */
/*
    private List<SumDebRec> getRolledDebPen(CalcStore calcStore, List<SumDebRec> lstGroupedDebt, UslOrg uslOrg, boolean isLastDay) {
        // отсортировать по периоду
        List<SumDebRec> lstSorted = lstGroupedDebt.stream()
                .sorted(Comparator.comparing(SumDebRec::getMg))
                .collect(Collectors.toList());

        // свернуть задолженность
        BigDecimal ovrPay = BigDecimal.ZERO;
        BigDecimal ovrPayDeb = BigDecimal.ZERO;

        Iterator<SumDebRec> itr = lstSorted.iterator();
        while (itr.hasNext()) {
            SumDebRec t = itr.next();
            // дата расчета
            t.setDt(curDt);
            // Id услуги
            t.setUslId(uslOrg.getUslId());
            // Id организации
            t.setOrgId(uslOrg.getOrgId());
            // пометить текущий день, является ли последним расчетным днём
            t.setIsLastDay(isLastDay);
            // Задолженность для расчета ПЕНИ
            // взять сумму текущего периода, добавить переплату
            BigDecimal summa = t.getSumma().add(ovrPay);
            if (summa.compareTo(BigDecimal.ZERO) <= 0) {
                // переплата или 0
                if (itr.hasNext()) {
                    // перенести переплату в следующий период
                    ovrPay = summa;
                    t.setSumma(BigDecimal.ZERO);
                } else {
                    // последний период, записать сумму с учетом переплаты
                    ovrPay = BigDecimal.ZERO;
                    t.setSumma(summa);
                }
            } else {
                // остался долг, записать его
                ovrPay = BigDecimal.ZERO;
                t.setSumma(summa);

                // если установлена дата ограничения пени, не считать пеню, начиная с этой даты
				*/
/*if (kart.getPnDt() == null || curDt.getTime() < kart.getPnDt().getTime()) {
					// рассчитать пеню и сохранить
					Pen pen = getPen(summa, t.getMg(), kart);
					if (pen != null) {
						// сохранить текущую пеню
						t.setPenyaChrg(pen.penya);
						// кол-во дней просрочки
						t.setDays(pen.days);
						// % расчета пени
						t.setProc(pen.proc);
					}
				}*//*

            }

            if (isLastDay) {
                // Актуально для последнего расчетного дня
                // Задолженность для отображения клиенту (свернутая)
                // взять сумму текущего периода, добавить переплату
                summa = t.getDebRolled().add(ovrPayDeb);
                if (summa.compareTo(BigDecimal.ZERO) <= 0) {
                    // переплата или 0
                    if (itr.hasNext()) {
                        // перенести переплату в следующий период
                        ovrPayDeb = summa;
                        t.setDebRolled(BigDecimal.ZERO);
                    } else {
                        // последний период, записать сумму с учетом переплаты
                        ovrPayDeb = BigDecimal.ZERO;
                        t.setDebRolled(summa);
                    }
                } else {
                    // остался долг, записать его
                    ovrPayDeb = BigDecimal.ZERO;
                    t.setDebRolled(summa);
                }
            }

        }
        if (calcStore.getDebugLvl().equals(1)) {
            log.info("");
            lstSorted.forEach(t-> {
                log.info("СВЕРНУТЫЕ долги: usl={}, org={}, дата={} период={}", t.getUslId(), t.getOrgId(),
                        Utl.getStrFromDate(curDt, "dd.MM.yyyy"), t.getMg());
                log.info("долг для пени={}, долг={}, свернутый долг={},",
                        t.getSumma(), t.getDebOut(), t.getDebRolled());
            });
        }

        return lstSorted;
    }
*/

}