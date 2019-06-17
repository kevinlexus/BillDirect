package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.bill.dto.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Service;

import com.dic.app.mm.DebitThrMng;
import com.dic.bill.model.scott.Kart;

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

    @Getter
    @Setter
    class DebPeriod {
        private String uslId;
        private Integer orgId;
        private Integer mg;

        private DebPeriod(String uslId, Integer orgId, Integer mg) {
            this.uslId = uslId;
            this.orgId = orgId;
            this.mg = mg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DebPeriod)) return false;
            DebPeriod debPeriod = (DebPeriod) o;
            return Objects.equals(uslId, debPeriod.uslId) &&
                    Objects.equals(orgId, debPeriod.orgId) &&
                    Objects.equals(mg, debPeriod.mg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uslId, orgId, mg);
        }
    }

    @Getter
    @Setter
    class PeriodSumma {
        // задолженность
        private BigDecimal deb;
        // задолженность для расчета пени
        private BigDecimal debForPen;

        public PeriodSumma(BigDecimal deb, BigDecimal debForPen) {
            this.deb = deb;
            this.debForPen = debForPen;
        }
    }


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

        // долги предыдущего периода (вх.сальдо)
        Map<DebPeriod, PeriodSumma> mapDebPart1 = new HashMap<>();
        localStore.getLstDebFlow()
                .forEach(t -> mapDebPart1.put(new DebPeriod(t.getUslId(), t.getOrgId(), t.getMg()),
                        new PeriodSumma(t.getDebOut(), t.getDebOut())));

        // текущее начисление
        process(localStore.getLstChrgFlow().stream(), mapDebPart1, null, null, false, calcStore.getPeriod());

        // Сгруппировать все операции по усл+орг+период
        HashMap<DebPeriod, PeriodSumma> mapDebPart2 = null;
        // перебрать все дни с начала месяца по дату расчета, включительно
        Calendar c = Calendar.getInstance();
        for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();

            // восстановить неизменную часть
            mapDebPart2 =
                    mapDebPart1.entrySet().stream().collect(Collectors.toMap(
                            k -> new DebPeriod(k.getKey().getUslId(), k.getKey().getOrgId(), k.getKey().getMg()),
                            v -> new PeriodSumma(v.getValue().getDeb(), v.getValue().getDebForPen()),
                            (k, v) -> k, HashMap::new));

            // перерасчеты, включая текущий день
            process(localStore.getLstChngFlow().stream(), mapDebPart2, curDt, null, false, null);

            // вычесть оплату долга, включая текущий день поступления - для обычного долга
            // и не включая для расчета пени
            process(localStore.getLstPayFlow().stream(), mapDebPart2, curDt, curDt, true, null);

            // вычесть корректировки оплаты - для расчета долга, включая текущий день
            process(localStore.getLstPayCorrFlow().stream(), mapDebPart2, curDt, null, true, null);

            log.info("********** Долги на дату: dt={}, lsk={}", curDt, kart.getLsk());
            mapDebPart2.forEach((key, value) -> {
                if (key.getUslId().equals("011") && key.getOrgId().equals(3)) {
                    log.info("долг: usl={}, org={}, mg={}, deb={}, debForPen={}",
                            key.getUslId(), key.getOrgId(), key.getMg(),
                            value.getDeb(), value.getDebForPen());
                    log.info("equals={}", key.getUslId().equals("011"));
                }
            });


            // Перенести переплату
            // уникальные значения Usl, Org
            Map<String, Integer> mapUslOrg = mapDebPart2.entrySet().stream()
                    .collect(Collectors.toMap(k -> k.getKey().getUslId(), v -> v.getKey().getOrgId(), (k, v) -> k));

            for (Map.Entry<String, Integer> entry : mapUslOrg.entrySet()) {
                // отсортировать по периоду
                List<Map.Entry<DebPeriod, PeriodSumma>> mapSorted =
                        mapDebPart2.entrySet().stream()
                                .filter(t -> t.getKey().getUslId().equals(entry.getKey())
                                        && t.getKey().getOrgId().equals(entry.getValue()))
                                .sorted(Comparator.comparing(t -> t.getKey().getMg()))
                                .collect(Collectors.toList());


                log.info("Осортировано: usl={}, org={}", entry.getKey(), entry.getValue());
                mapSorted.forEach(t -> log.info("check mg={}, deb={}, debForPen={}",
                        t.getKey().getMg(), t.getValue().getDeb(), t.getValue().getDebForPen()));


                // перенести переплату
                BigDecimal overPay = BigDecimal.ZERO;
                BigDecimal overPayForPen = BigDecimal.ZERO;
                ListIterator<Map.Entry<DebPeriod, PeriodSumma>> itr = mapSorted.listIterator();
                while (itr.hasNext()) {
                    Map.Entry<DebPeriod, PeriodSumma> t = itr.next();

                    // долг
                    if (itr.hasNext()) {
                        // не последний период, перенести переплату, если есть
                        if (overPay.add(t.getValue().getDeb()).compareTo(BigDecimal.ZERO) < 0) {
                            overPay = overPay.add(t.getValue().getDeb());
                            t.getValue().setDeb(BigDecimal.ZERO);
                        } else {
                            t.getValue().setDeb(overPay.add(t.getValue().getDeb()));
                            overPay = BigDecimal.ZERO;
                        }
                    } else {
                        // последний период
                        if (overPay.compareTo(BigDecimal.ZERO) != 0) {
                            t.getValue().setDeb(overPay.add(t.getValue().getDeb()));
                        }
                    }

                    // долг для расчета пени
                    if (itr.hasNext()) {
                        // не последний период, перенести переплату, если есть
                        if (overPayForPen.add(t.getValue().getDebForPen()).compareTo(BigDecimal.ZERO) < 0) {
                            overPayForPen = overPayForPen.add(t.getValue().getDebForPen());
                            t.getValue().setDebForPen(BigDecimal.ZERO);
                        } else {
                            t.getValue().setDebForPen(overPayForPen.add(t.getValue().getDebForPen()));
                            overPayForPen = BigDecimal.ZERO;
                        }
                    } else {
                        // последний период
                        if (overPayForPen.compareTo(BigDecimal.ZERO) != 0) {
                            t.getValue().setDebForPen(overPayForPen.add(t.getValue().getDebForPen()));
                        }
                    }
                }
            }

            mapDebPart2.entrySet().stream().sorted((Comparator.comparing(o -> o.getKey().getMg())))
                    .forEach(t -> {
                        if (t.getKey().getUslId().equals("011") && t.getKey().getOrgId().equals(3)) {
                            log.info("Свернуто: usl={}, org={}, mg={}, deb={}, debForPen={}",
                                    t.getKey().getUslId(), t.getKey().getOrgId(), t.getKey().getMg(),
                                    t.getValue().getDeb(), t.getValue().getDebForPen());
                        }
                    });

        }
    }

    /**
     * Обработка финансового потока
     *
     * @param stream         - поток
     * @param mapDeb         - результат
     * @param beforeDt       - ограничивать до даты, включительно
     * @param beforeDtForPen - ограничивать до даты, не включая, для пени
     * @param isNegate       - делать отрицательный знак (для оплаты)
     * @param curMg          - текущий период
     */
    private void process(Stream<SumRec> stream, Map<DebPeriod, PeriodSumma> mapDeb,
                         Date beforeDt, Date beforeDtForPen, boolean isNegate, Integer curMg) {
        stream
                .filter(t -> beforeDt == null || t.getDt().getTime() <= beforeDt.getTime()) // ограничить по дате
                .forEach(t -> {
                            DebPeriod debPeriod = new DebPeriod(
                                    t.getUslId(),
                                    t.getOrgId(),
                                    curMg != null ? curMg : t.getMg());
                            BigDecimal debForPen = BigDecimal.ZERO;
                            // ограничить по дате для долга по пене
                            if (beforeDtForPen == null || t.getDt().getTime() < beforeDtForPen.getTime()) {
                                debForPen = isNegate ? t.getSumma().negate() : t.getSumma();
                            }
                            PeriodSumma periodSumma =
                                    new PeriodSumma(isNegate ? t.getSumma().negate() : t.getSumma(), debForPen
                                    );

                            PeriodSumma val = mapDeb.get(debPeriod);
                            if (val == null) {
                                mapDeb.put(debPeriod, periodSumma);
                            } else {
                                val.setDeb(val.getDeb().add(periodSumma.getDeb()));
                                val.setDebForPen(val.getDebForPen().add(periodSumma.getDebForPen()));
                            }
                        }
                );

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