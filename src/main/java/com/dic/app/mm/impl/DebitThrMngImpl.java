package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.app.mm.GenPenMng;
import com.dic.bill.dao.PenCurDAO;
import com.dic.bill.dto.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Service;

import com.dic.app.mm.DebitThrMng;

import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.toMap;

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
    private final GenPenMng genPenMng;
    private final PenCurDAO penCurDAO;

    public DebitThrMngImpl(EntityManager em, GenPenMng genPenMng, PenCurDAO penCurDAO) {
        this.em = em;
        this.genPenMng = genPenMng;
        this.penCurDAO = penCurDAO;
    }

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
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
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

        HashMap<DebPeriod, PeriodSumma> mapDebPart2 = null;
        // долги для расчета пени по дням - совокупно все услуги - упорядоченный по элементам LinkedHashMap
        Map<Date, Map<Integer, BigDecimal>> mapDebForPen = new LinkedHashMap<>();
        // перебрать все дни с начала месяца по дату расчета, включительно
        Calendar c = Calendar.getInstance();
        for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
            Date dt = c.getTime();

            // восстановить неизменную часть
            mapDebPart2 =
                    mapDebPart1.entrySet().stream().collect(toMap(
                            k -> new DebPeriod(k.getKey().getUslId(), k.getKey().getOrgId(), k.getKey().getMg()),
                            v -> new PeriodSumma(v.getValue().getDeb(), v.getValue().getDebForPen()),
                            (k, v) -> k, HashMap::new));

            // перерасчеты, включая текущий день
            process(localStore.getLstChngFlow().stream(), mapDebPart2, dt, null, false, null);

            // вычесть оплату долга, включая текущий день поступления - для обычного долга
            // и не включая для расчета пени
            process(localStore.getLstPayFlow().stream(), mapDebPart2, dt, dt, true, null);

            // вычесть корректировки оплаты - для расчета долга, включая текущий день
            process(localStore.getLstPayCorrFlow().stream(), mapDebPart2, dt, null, true, null);

            log.info("********** Долги на дату: dt={}, lsk={}", dt, kart.getLsk());


            // TODO Сохранить в DEB несвернутые долги на последнюю дату расчета!
            if (dt.getTime() == dt2.getTime()) {
                mapDebPart2.forEach((k,v)-> saveDeb(calcStore, kart, localStore,
                        k.getUslId(), k.getOrgId(), k.getMg(), v.deb));
            }



            /*
            mapDebPart2.forEach((key, value) -> {
                if (key.getUslId().equals("011") && key.getOrgId().equals(3)) {
                    log.info("долг: usl={}, org={}, mg={}, deb={}, debForPen={}",
                            key.getUslId(), key.getOrgId(), key.getMg(),
                            value.getDeb(), value.getDebForPen());
                }
            });*/
            // перенести переплату
            moveOverpay(mapDebPart2);

            mapDebPart2.entrySet().stream().sorted((Comparator.comparing(o -> o.getKey().getMg())))
                    .forEach(t -> {
                        //if (t.getKey().getUslId().equals("011") && t.getKey().getOrgId().equals(3)) {
                        log.info("Свернуто: usl={}, org={}, mg={}, deb={}, debForPen={}",
                                t.getKey().getUslId(), t.getKey().getOrgId(), t.getKey().getMg(),
                                t.getValue().getDeb(), t.getValue().getDebForPen());
                        //}
                    });

            // сгруппировать сумму свернутых долгов для расчета пени по всем услугам, по датам
            groupByDateMg(mapDebPart2, mapDebForPen, dt);

        }
        // рассчитать пеню
        genPen(kart, calcStore, mapDebForPen);

    }


    /**
     * Сохранить запись долга
     * @param calcStore - хранилище справочников
     * @param kart - лиц.счет
     * @param localStore  - хранилище всех операций по лиц.счету
     * @param uslId - ID услуги
     * @param orgId - ID организации
     * @param mg - период
     * @param debOut - долг
     */
    private void saveDeb(CalcStore calcStore, Kart kart, CalcStoreLocal localStore, String uslId,
                         int orgId, int mg, BigDecimal debOut) {
        // флаг создания новой записи
        boolean isCreate = false;
        // найти запись долгов предыдущего периода
        SumDebPenRec foundDeb = localStore.getLstDebFlow().stream()
                .filter(d -> d.getUslId().equals(uslId))
                .filter(d -> d.getOrgId().equals(orgId))
                .filter(d -> d.getMg().equals(mg))
                .findFirst().orElse(null);
        if (foundDeb == null) {
            // не найдена, создать новую запись
            isCreate = true;
        } else {
            // найдена, проверить равенство по полям
            if (debOut.compareTo(foundDeb.getDebOut()) ==0) {
                // равны, расширить период
                Deb deb = em.find(Deb.class, foundDeb.getId());
                deb.setMgTo(calcStore.getPeriod());
            } else {
                // не равны, создать запись нового периода
                isCreate = true;
            }
        }
        if (isCreate) {
            // создать запись нового периода
            if (debOut.compareTo(BigDecimal.ZERO) != 0) {
                // если хотя бы одно поле != 0
                Usl usl = em.find(Usl.class, uslId);
                if (usl == null) {
                    // так как внутри потока, то только RuntimeException
                    throw new RuntimeException("Ошибка при сохранении записей долгов,"
                            + " некорректные данные в таблице SCOTT.DEB!"
                            + " Не найдена услуга с кодом usl=" + uslId);
                }
                Org org = em.find(Org.class, orgId);
                if (org == null) {
                    // так как внутри потока, то только RuntimeException
                    throw new RuntimeException("Ошибка при сохранении записей долгов,"
                            + " некорректные данные в таблице SCOTT.DEB!"
                            + " Не найдена организация с кодом org=" + orgId);
                }
                Deb deb = Deb.builder()
                        .withUsl(usl)
                        .withOrg(org)
                        .withDebOut(debOut)
                        .withKart(kart)
                        .withMgFrom(calcStore.getPeriod())
                        .withMgTo(calcStore.getPeriod())
                        .withMg(mg)
                        .build();
                kart.getDeb().add(deb);
                em.persist(deb);
            }
        }
    }

    /**
     * Рассчитать пеню
     *
     * @param kart         - текущий лиц.счет
     * @param calcStore    - хранилище справочников
     * @param mapDebForPen - долги для расчета пени
     */
    private void genPen(Kart kart, CalcStore calcStore, Map<Date, Map<Integer, BigDecimal>> mapDebForPen) {
        // расчитать пеню по долгам
        // кол-во дней начисления пени на дату расчета, по периодам задолженности
        Map<Integer, Integer> mapPenDays = new HashMap<>();
        // сумма пени на дату расчета, по периодам задолженности
        Map<Integer, BigDecimal> mapPen = new HashMap<>();
        for (Map.Entry<Date, Map<Integer, BigDecimal>> dtEntry : mapDebForPen.entrySet()) {
            Date dt = dtEntry.getKey();
            for (Map.Entry<Integer, BigDecimal> entry : dtEntry.getValue().entrySet()) {
                // получить одну запись долга по дате
                BigDecimal debForPen = entry.getValue();
                if (debForPen.compareTo(BigDecimal.ZERO) > 0) {
                    Integer mg = entry.getKey();
                    // расчет пени
                    GenPenMngImpl.PenDTO penDto = genPenMng.getPen(calcStore, debForPen, mg, kart, dt);
                    if (penDto != null) {
                        // добавить 1 день расчета пени
                        mapPenDays.merge(mg, 1, (k, v) -> k + 1);
                        // добавить сумму пени за 1 день
                        mapPen.merge(mg, penDto.getPenya(), BigDecimal::add);
                        log.info("Пеня: debForPen={}, dt={}, mg={}, days={}, penya={}, proc={}",
                                debForPen, Utl.getStrFromDate(dt), mg, penDto.getDays(), penDto.getPenya(), penDto.getProc());
                    }
                }
            }
        }


        // сохранить пеню в C_PEN_CUR
        penCurDAO.deleteByLsk(kart.getLsk());
        mapPen.forEach((key, value) -> {
                    Integer days = mapPenDays.get(key);
                    PenCur penCur = new PenCur();
                    penCur.setKart(kart);
                    penCur.setMg1(String.valueOf(key));
                    penCur.setCurDays(days);
                    penCur.setPenya(value);
                    kart.getPenCur().add(penCur);
                    em.persist(penCur);
                    log.info("Итого: период={}, тек.пеня={}, тек.дней={}", key, value, days);
                }
        );


    }

    /**
     * Сгруппировать долг для расчета пени по дате, периоду
     *
     * @param mapDebPart2  - исходная коллекция
     * @param mapDebForPen - результат
     * @param curDt        - дата группировки
     */
    private void groupByDateMg(HashMap<DebPeriod, PeriodSumma> mapDebPart2, Map<Date, Map<Integer, BigDecimal>> mapDebForPen, Date curDt) {
        // взять только положительную составляющую, так как для данного периода долга нужно
        // брать только задолженности по услугам, но не переплаты
        mapDebPart2.entrySet().stream()
                .filter(t -> t.getValue().getDebForPen().compareTo(BigDecimal.ZERO) > 0)
                .forEach(t -> {
                    Map<Integer, BigDecimal> mapByDt = mapDebForPen.get(curDt);
                    if (mapByDt != null) {
                        mapByDt.merge(t.getKey().getMg(), t.getValue().getDebForPen(), BigDecimal::add);
                    } else {
                        Map<Integer, BigDecimal> map = new HashMap<>();
                        map.put(t.getKey().getMg(), t.getValue().getDebForPen());
                        mapDebForPen.put(curDt, map);
                    }
                });
    }

    /**
     * Перенести переплату
     *
     * @param mapDebPart2 - коллекция для обработки
     */
    private void moveOverpay(HashMap<DebPeriod, PeriodSumma> mapDebPart2) {
        // уникальные значения Usl, Org
        Map<String, Integer> mapUslOrg = mapDebPart2.entrySet().stream()
                .collect(toMap(k -> k.getKey().getUslId(), v -> v.getKey().getOrgId(), (k, v) -> k));

        for (Map.Entry<String, Integer> entry : mapUslOrg.entrySet()) {
            // отсортировать по периоду
            List<Map.Entry<DebPeriod, PeriodSumma>> mapSorted =
                    mapDebPart2.entrySet().stream()
                            .filter(t -> t.getKey().getUslId().equals(entry.getKey())
                                    && t.getKey().getOrgId().equals(entry.getValue()))
                            .sorted(Comparator.comparing(t -> t.getKey().getMg()))
                            .collect(Collectors.toList());


            //log.info("Осортировано: usl={}, org={}", entry.getKey(), entry.getValue());
            //mapSorted.forEach(t -> log.info("check mg={}, deb={}, debForPen={}",
            //        t.getKey().getMg(), t.getValue().getDeb(), t.getValue().getDebForPen()));


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
}