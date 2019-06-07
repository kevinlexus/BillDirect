package com.dic.app.mm.impl;

import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.GenPenProcessMng;
import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.*;
import com.dic.bill.dto.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.ListIterator;

/**
 * Сервис формирования задолженностей и пени
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
@Scope("prototype")
public class GenPenProcessMngImpl implements GenPenProcessMng {

    private final DebDAO debDao;
    private final PenDAO penDao;
    private final ChargeDAO chargeDao;
    private final VchangeDetDAO vchangeDetDao;
    private final KwtpDayDAO kwtpDayDao;
    private final CorrectPayDAO correctPayDao;
    private final PenUslCorrDAO penUslCorrDao;
    private final ReferenceMng refMng;
    private final DebitThrMng debitThrMng;

    @PersistenceContext
    private EntityManager em;

    public GenPenProcessMngImpl(DebDAO debDao, PenDAO penDao, ChargeDAO chargeDao,
                                VchangeDetDAO vchangeDetDao, KwtpDayDAO kwtpDayDao,
                                CorrectPayDAO correctPayDao, PenUslCorrDAO penUslCorrDao,
                                ReferenceMng refMng, DebitThrMng debitThrMng) {
        this.debDao = debDao;
        this.penDao = penDao;
        this.chargeDao = chargeDao;
        this.vchangeDetDao = vchangeDetDao;
        this.kwtpDayDao = kwtpDayDao;
        this.correctPayDao = correctPayDao;
        this.penUslCorrDao = penUslCorrDao;
        this.refMng = refMng;
        this.debitThrMng = debitThrMng;
    }

    /**
     * Рассчет задолженности и пени по всем лиц.счетам помещения
     *
     * @param calcStore - хранилище объемов, справочников
     * @param isCalcPen - рассчитывать пеню?
     * @param klskId    - klskId помещения
     */
    @Override
    public void genDebitPen(CalcStore calcStore, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen {
        Ko ko = em.find(Ko.class, klskId);
        for (Kart kart : ko.getKart()) {
            genDebitPen(calcStore, isCalcPen, kart);
        }
    }

    /**
     * Рассчет задолженности и пени по лиц.счету
     *
     * @param calcStore - хранилище справочников
     * @param isCalcPen - рассчитывать пеню?
     * @param kart      - лиц.счет
     */
    private void genDebitPen(CalcStore calcStore, boolean isCalcPen, Kart kart) throws ErrorWhileChrgPen {
        Integer period = calcStore.getPeriod();
        Integer periodBack = calcStore.getPeriodBack();
        // ЗАГРУЗИТЬ все финансовые операции по лиц.счету
        CalcStoreLocal localStore = new CalcStoreLocal();
        // задолженность предыдущего периода
        localStore.setLstDebFlow(debDao.getDebitByLsk(kart.getLsk(), periodBack));
        // текущее начисление - 2
        localStore.setLstChrgFlow(chargeDao.getChargeByLsk(kart.getLsk()));
        // перерасчеты - 5
        localStore.setLstChngFlow(vchangeDetDao.getVchangeDetByLsk(kart.getLsk()));
        // оплата долга - 3
        localStore.setLstPayFlow(kwtpDayDao.getKwtpDaySumByLsk(kart.getLsk()));
        // корректировки оплаты - 6
        localStore.setLstPayCorrFlow(correctPayDao.getCorrectPayByLsk(kart.getLsk(), String.valueOf(period)));
        // создать список уникальных элементов услуга+организация
        localStore.createUniqUslOrg();
        // преобразовать String код reu в int, для ускорения фильтров
        localStore.setReuId(Integer.parseInt(kart.getUk().getReu()));
        // получить список уникальных элементов услуга+организация
        List<UslOrg> lstUslOrg = localStore.getUniqUslOrg();


        lstUslOrg.forEach(t-> log.info("usl={}, org={}", t.getUslId(), t.getOrgId()));


        // Расчет задолженности, подготовка для расчета пени
        debitThrMng.genDebitUsl(kart, calcStore, localStore);


/*
        // Расчет пени
        List<SumDebRec> lstPen;
        try {
            lstPen = debitThrMng.genDebitUsl(kart, null, calcStore, localStore, isCalcPen);
        } catch (ErrorWhileChrgPen e) {
            log.error(Utl.getStackTraceString(e));
            throw new RuntimeException("ОШИБКА во время расчета пени, лc.=" + kart.getLsk());
        }


        lstPen.forEach(t-> {
            log.info("TEST: dt={}, mg={}, usl={}, org={}, PenyaIn={}, PenyaCorr={}, PenyaPay={}, PenyaChrg={}, PenChrgCorr={}",
                    t.getDt(), t.getMg(), t.getUslId(), t.getOrgId(), t.getPenyaIn(),
                    t.getPenyaCorr(), t.getPenyaPay(), t.getPenyaChrg(), t.getPenChrgCorr());
        });
*/
        /*List<SumPenRec> lstGrp;
        if (isCalcPen) {
            // найти совокупные задолженности каждого дня, обнулить пеню, в тех днях, где задолженность = 0
            // по дням
            Calendar c = Calendar.getInstance();
            for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getGenDt()); c.add(Calendar.DATE, 1)) {
                Date curDt = c.getTime();
                // суммировать по дате
                BigDecimal debForPen = lst.stream().filter(t -> t.getDt().equals(curDt))
                        .map(SumDebRec::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
                if (debForPen.compareTo(BigDecimal.ZERO) <= 0) {
                    // нет долгов, занулить пеню по всей дате
                    lst.stream().filter(t -> t.getDt().equals(curDt)).forEach(t -> t.setPenyaChrg(BigDecimal.ZERO));
                }
            }

            // СГРУППИРОВАТЬ ПЕНЮ ПО ПЕРИОДАМ
            try {
                lstGrp = getGroupingPenDeb(lstPen, isCalcPen);
            } catch (ErrorWhileChrgPen e) {
                log.error(Utl.getStackTraceString(e));
                throw new RuntimeException("ОШИБКА во время итоговой группировки пени по периодам, лc.=" + kart.getLsk());
            }

            // перенаправить пеню на услугу и организацию по справочнику REDIR_PAY (как перенаправлять, если usl==null? ред.25.05.2019)
            //redirectPen(kart, lstGrp);

            // удалить записи текущего периода, если они были созданы
            penDao.delByLskPeriod(kart.getLsk(), period);
            penDao.updByLskPeriod(kart.getLsk(), period, periodBack);

        } else {
            try {
                lstGrp = getGroupingPenDeb(lst, isCalcPen);
            } catch (ErrorWhileChrgPen e) {
                log.error(Utl.getStackTraceString(e));
                throw new RuntimeException("ОШИБКА во время итоговой группировки пени по периодам, лc.=" + kart.getLsk());
            }

        } */

        /*
        // обновить mgTo записей, если они были расширены до текущего периода
        debDao.delByLskPeriod(kart.getLsk(), period);
        debDao.updByLskPeriod(kart.getLsk(), period, periodBack);

        // получить задолженность, по которой расчитывается пеня, по всем услугам
        for (SumPenRec t : lstGrp) {
            // рассчитать исходящее сальдо по пене, сохранить расчет, задолженность
            save(calcStore, kart, localStore, t, isCalcPen);
        }*/
    }

    /**
     * Сохранить расчет
     *
     * @param calcStore  - хранилище справочников
     * @param kart       - лиц.счет
     * @param localStore - локальное хранилище финансовых операций по лиц.счету
     * @param sumPenRec  - расчитанная строка
     * @param isCalcPen  - учитывать пеню
     */
/*
    private void save(CalcStore calcStore, Kart kart, CalcStoreLocal localStore, SumPenRec sumPenRec, boolean isCalcPen) {
        // флаг создания новой записи
        boolean isCreate = false;
        // найти запись долгов предыдущего периода
        SumDebPenRec foundDeb = localStore.getLstDebFlow().stream()
                .filter(d -> d.getUslId().equals(sumPenRec.getUslId()))
                .filter(d -> d.getOrgId().equals(sumPenRec.getOrgId()))
                .filter(d -> d.getMg().equals(sumPenRec.getMg()))
                .findFirst().orElse(null);
        if (foundDeb == null) {
            // не найдена, создать новую запись
            isCreate = true;
        } else {
            // найдена, проверить равенство по полям
            if (Utl.isEqual(sumPenRec.getDebIn(), foundDeb.getDebIn())
                    && Utl.isEqual(sumPenRec.getDebOut(), foundDeb.getDebOut())
                    && Utl.isEqual(sumPenRec.getDebRolled(), foundDeb.getDebRolled())
                    && Utl.isEqual(sumPenRec.getChrg(), foundDeb.getChrg())
                    && Utl.isEqual(sumPenRec.getChng(), foundDeb.getChng())
                    && Utl.isEqual(sumPenRec.getDebPay(), foundDeb.getDebPay())
                    && Utl.isEqual(sumPenRec.getPayCorr(), foundDeb.getPayCorr())
            ) {
                // равны, расширить период
                //log.info("найти id={}", foundDeb.getId());
                Deb deb = em.find(Deb.class, foundDeb.getId());
                //log.info("найти deb={}", deb);
                deb.setMgTo(calcStore.getPeriod());
            } else {
                // не равны, создать запись нового периода
                isCreate = true;
            }
        }
        if (isCreate) {
            // создать запись нового периода
            if (sumPenRec.getDebIn().compareTo(BigDecimal.ZERO) != 0
                    || sumPenRec.getDebOut().compareTo(BigDecimal.ZERO) != 0
                    || sumPenRec.getDebRolled().compareTo(BigDecimal.ZERO) != 0
                    || sumPenRec.getChrg().compareTo(BigDecimal.ZERO) != 0
                    || sumPenRec.getChng().compareTo(BigDecimal.ZERO) != 0
                    || sumPenRec.getDebPay().compareTo(BigDecimal.ZERO) != 0
                    || sumPenRec.getPayCorr().compareTo(BigDecimal.ZERO) != 0
            ) {
                // если хотя бы одно поле != 0
                Usl usl = em.find(Usl.class, sumPenRec.getUslId());
                if (usl == null) {
                    // так как внутри потока, то только RuntimeException
                    throw new RuntimeException("Ошибка при сохранении записей долгов,"
                            + " некорректные данные в таблице SCOTT.DEB!"
                            + " Не найдена услуга с кодом usl=" + sumPenRec.getUslId());
                }
                Org org = em.find(Org.class, sumPenRec.getOrgId());
                if (org == null) {
                    // так как внутри потока, то только RuntimeException
                    throw new RuntimeException("Ошибка при сохранении записей долгов,"
                            + " некорректные данные в таблице SCOTT.DEB!"
                            + " Не найдена организация с кодом org=" + sumPenRec.getOrgId());
                }
                Deb deb = Deb.builder()
                        .withUsl(usl)
                        .withOrg(org)
                        .withDebIn(sumPenRec.getDebIn())
                        .withDebOut(sumPenRec.getDebOut())
                        .withDebRolled(sumPenRec.getDebRolled())
                        .withChrg(sumPenRec.getChrg())
                        .withChng(sumPenRec.getChng())
                        .withDebPay(sumPenRec.getDebPay())
                        .withPayCorr(sumPenRec.getPayCorr())
                        .withKart(kart)
                        .withMgFrom(calcStore.getPeriod())
                        .withMgTo(calcStore.getPeriod())
                        .withMg(sumPenRec.getMg())
                        .build();
                em.persist(deb);
            }
        }

        if (isCalcPen) {
            // округлить начисленную пеню
            BigDecimal penChrgRound = sumPenRec.getPenyaChrg().setScale(2, RoundingMode.HALF_UP);
            // исх.сальдо по пене
            BigDecimal penyaOut = sumPenRec.getPenyaIn().add(penChrgRound)
                    .add(sumPenRec.getPenyaCorr())     // прибавить корректировки
                    .subtract(sumPenRec.getPenyaPay()  // отнять оплату
                    );
            if (calcStore.getDebugLvl().equals(1)) {
                log.info("uslId={}, orgId={}, период={}, долг={}, свернутый долг={}, "
                                + "пеня вх.={}, пеня тек.={} руб., корр.пени={}, пеня исх.={}, дней просрочки(на дату расчета)={}",
                        sumPenRec.getUslId(), sumPenRec.getOrgId(), sumPenRec.getMg(),
                        sumPenRec.getDebOut(), sumPenRec.getDebRolled(), sumPenRec.getPenyaIn(),
                        sumPenRec.getPenyaChrg(), sumPenRec.getPenyaCorr(), penyaOut,
                        sumPenRec.getDays());
            }

            // сбросить флаг создания новой записи
            isCreate = false;

            // найти запись пени предыдущего периода
            SumDebPenRec foundPen = localStore.getLstDebPenFlow().stream()
                    .filter(d -> d.getUslId().equals(sumPenRec.getUslId()))
                    .filter(d -> d.getOrgId().equals(sumPenRec.getOrgId()))
                    .filter(d -> d.getMg().equals(sumPenRec.getMg()))
                    .findFirst().orElse(null);
            if (foundPen == null) {
                // не найдена, создать новую запись
                isCreate = true;
            } else {
                // найдена, проверить равенство по полям
                if (Utl.isEqual(sumPenRec.getPenyaIn(), foundPen.getPenIn())
                        && Utl.isEqual(penyaOut, foundPen.getPenOut())
                        && Utl.isEqual(penChrgRound, foundPen.getPenChrg())
                        && Utl.isEqual(sumPenRec.getPenyaCorr(), foundPen.getPenCorr())
                        && Utl.isEqual(sumPenRec.getPenyaPay(), foundPen.getPenPay())
                        && Utl.isEqual(sumPenRec.getDays(), foundPen.getDays())
                ) {
                    // равны, расширить период
                    Pen pen = em.find(Pen.class, foundPen.getId());
                    pen.setMgTo(calcStore.getPeriod());
                } else {
                    // не равны, создать запись нового периода
                    isCreate = true;
                }

            }

            if (isCreate) {
                // создать запись нового периода
                if (sumPenRec.getPenyaIn().compareTo(BigDecimal.ZERO) != 0
                        || penyaOut.compareTo(BigDecimal.ZERO) != 0
                        || penChrgRound.compareTo(BigDecimal.ZERO) != 0
                        || sumPenRec.getPenyaCorr().compareTo(BigDecimal.ZERO) != 0
                        || sumPenRec.getPenyaPay().compareTo(BigDecimal.ZERO) != 0
                    //|| !t.getDays().equals(0)
                ) {
                    // если хотя бы одно поле != 0
                    Usl usl = em.find(Usl.class, sumPenRec.getUslId());
                    if (usl == null) {
                        // так как внутри потока, то только RuntimeException
                        throw new RuntimeException("Ошибка при сохранении записей пени,"
                                + " некорректные данные в таблице SCOTT.PEN!"
                                + " Не найдена услуга с кодом usl=" + sumPenRec.getUslId());
                    }
                    Org org = em.find(Org.class, sumPenRec.getOrgId());
                    if (org == null) {
                        // так как внутри потока, то только RuntimeException
                        throw new RuntimeException("Ошибка при сохранении записей пени,"
                                + " некорректные данные в таблице SCOTT.PEN!"
                                + " Не найдена организация с кодом org=" + sumPenRec.getOrgId());
                    }

                    Pen pen = Pen.builder()
                            .withUsl(usl)
                            .withOrg(org)
                            .withPenIn(sumPenRec.getPenyaIn())
                            .withPenOut(penyaOut)
                            .withPenChrg(penChrgRound)
                            .withPenCorr(sumPenRec.getPenyaCorr())
                            .withPenPay(sumPenRec.getPenyaPay())
                            .withDays(sumPenRec.getDays())
                            .withKart(kart)
                            .withMgFrom(calcStore.getPeriod())
                            .withMgTo(calcStore.getPeriod())
                            .withMg(sumPenRec.getMg())
                            .build();
                    em.persist(pen);
                }
            }
        }
    }
*/

    /**
     * Сгруппировать по периодам пеню, и долги на дату расчета
     *
     * @param lst       - долги по всем дням
     * @param isCalcPen - учитывать пеню
     */
    private List<SumPenRec> getGroupingPenDeb(List<SumDebRec> lst, boolean isCalcPen) throws ErrorWhileChrgPen {
        // получить долги на последнюю дату
/*
        List<SumPenRec> lstDebAmnt = lst.stream()
                .filter(SumDebRec::getIsLastDay)
                .map(t -> new SumPenRec(t.getDebIn(), t.getPayCorr(), t.getDebPay(),
                        t.getChrg(), t.getChng(), t.getUslId(), t.getOrgId(), t.getDebOut(),
                        t.getDebRolled(), t.getMg()))
                .collect(Collectors.toList());
*/

        if (isCalcPen) {
            // сгруппировать начисленную пеню по периодам
            for (SumDebRec t : lst) {
                //addPen(lstDebAmnt, t.getMg(), t.getPenyaChrg());
            }
        }
        return null;
//        return lstDebAmnt;
    }

    /**
     * добавить пеню по периоду в долги по последней дате
     * @param lstDebAmnt - коллекция долгов
     * @param mg         - период долга
     * @param penya      - начисленая пеня за день
     */
    private void addPen(List<SumPenRec>
                                lstDebAmnt, Integer mg, BigDecimal penya) throws ErrorWhileChrgPen {
        // найти запись долга с данным периодом
        SumPenRec recDeb = lstDebAmnt.stream()
                .filter(t -> t.getMg().equals(mg))
                .findFirst().orElse(null);
        if (recDeb != null) {
            // запись найдена, сохранить значение пени
            recDeb.setPenyaChrg(recDeb.getPenyaChrg().add(penya));
        } else {
            // должна быть найдена запись, иначе, ошибка в коде!
            throw new ErrorWhileChrgPen("Не найдена запись долга в процессе сохранения значения пени!");
        }
    }


    /**
     * перенаправить пеню на услугу и организацию по справочнику REDIR_PAY
     *
     * @param kart - лицевой счет
     * @param lst  - входящая коллекция долгов и пени
     */
    private void redirectPen(Kart kart, List<SumPenRec> lst) {
        // произвести перенаправление начисления пени, по справочнику
        ListIterator<SumPenRec> itr = lst.listIterator();
        while (itr.hasNext()) {
            SumPenRec t = itr.next();
            String uslId = t.getUslId();
            Integer orgId = t.getOrgId();
            UslOrg uo = refMng.getUslOrgRedirect(t.getUslId(), t.getOrgId(), kart, 0);
            if (!uo.getUslId().equals(uslId)
                    || !uo.getOrgId().equals(orgId)) {
                // выполнить переброску, если услуга или организация - другие
                SumPenRec rec = lst.stream().filter(d ->
                        d.getUslId().equals(uo.getUslId())
                                && d.getOrgId().equals(uo.getOrgId()))
                        .filter(d -> d.getMg().equals(t.getMg())) // тот же период
                        .findFirst().orElse(null);
                //log.info("ПОИСК mg={}, usl={}, org={}", t.getMg(), uo.getUslId(), uo.getOrgId());
                if (rec == null) {
                    //log.info("ПЕРИОД mg={}, usl={}, org={} Не найден! size={}", t.getMg(), uo.getUslId(), uo.getOrgId(), tp.size());
                    // строка с долгом и пенёй не найдена по данному периоду, создать
                    SumPenRec sumPenRec = SumPenRec.builder()
                            .withChng(BigDecimal.ZERO)
                            .withChrg(BigDecimal.ZERO)
                            .withDebIn(BigDecimal.ZERO)
                            .withDebOut(BigDecimal.ZERO)
                            .withDebPay(BigDecimal.ZERO)
                            .withDebRolled(BigDecimal.ZERO)
                            .withPayCorr(BigDecimal.ZERO)
                            .withPenyaChrg(t.getPenyaChrg())
                            .withPenyaCorr(BigDecimal.ZERO)
                            .withPenyaIn(BigDecimal.ZERO)
                            .withPenyaPay(BigDecimal.ZERO)
                            .withDays(0)
                            .withMg(t.getMg())
                            .withUslId(uo.getUslId())
                            .withOrgId(uo.getOrgId())
                            .build();
                    itr.add(sumPenRec);
                } else {
                    // строка найдена
                    //log.info("mg={}, usl={}, org={} найден", t.getMg(), uo.getUslId(), uo.getOrgId());
                    rec.setPenyaChrg(rec.getPenyaChrg().add(t.getPenyaChrg()));
                }
                //log.info("Перенаправление пени сумма={}", t.getPenyaChrg());
                t.setPenyaChrg(BigDecimal.ZERO);
            }
        }
    }


}