package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dao.UslDAO;
import com.dic.bill.dto.*;
import com.dic.bill.mm.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис расчета начисления
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
public class GenChrgProcessMngImpl implements GenChrgProcessMng {

    @Autowired
    private NaborMng naborMng;
    @Autowired
    private StatesPrDAO statesPrDao;
    @Autowired
    private KartMng kartMng;
    @Autowired
    private KartPrMng kartPrMng;
    @Autowired
    private MeterMng meterMng;
    @Autowired
    private MeterDAO meterDao;
    @Autowired
    private UslDAO uslDao;
    @Autowired
    private ConfigApp config;
    @Autowired
    private SprParamMng sprParamMng;
    @PersistenceContext
    private EntityManager em;


    /**
     * Рассчитать начисление
     * Внимание! Расчет идёт по помещению (помещению), но информация группируется по лиц.счету(Kart)
     * так как теоретически может быть одинаковая услуга на разных лиц.счетах, но на одной помещению!
     * ОПИСАНИЕ: https://docs.google.com/document/d/1mtK2KdMX4rGiF2cUeQFVD4HBcZ_F0Z8ucp1VNK8epx0/edit
     *
     * @param reqConf - конфиг запроса
     * @param klskId  - klskId помещения
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRES_NEW,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class) //
    public void genChrg(RequestConfigDirect reqConf, long klskId) throws WrongParam, ErrorWhileChrg {
        // заблокировать объект Ko для расчета
        if (!config.getLock().aquireLockId(reqConf.getRqn(), 1, klskId, 60)) {
            throw new ErrorWhileChrg("ОШИБКА БЛОКИРОВКИ klskId=" + klskId);
        }
        try {
            //log.info("******* klskId={} заблокирован для расчета", klskId);

            CalcStore calcStore = reqConf.getCalcStore();
            //Ko ko = em.find(Ko.class, klskId); //note Разобраться что оставить!
            Ko ko = em.getReference(Ko.class, klskId);

            // создать локальное хранилище объемов
            ChrgCountAmountLocal chrgCountAmountLocal = new ChrgCountAmountLocal();
            log.info("****** {} помещения klskId={}, основной лиц.счет lsk={} - начало    ******",
                    reqConf.getTpName(), ko.getId());
            // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
            int parVarCntKpr =
                    Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"), 0D).intValue();
            // параметр учета проживающих для капремонта
            int parCapCalcKprTp =
                    Utl.nvl(sprParamMng.getN1("CAP_CALC_KPR_TP"), 0D).intValue();

            //ChrgCount chrgCount = new ChrgCount();
            // выбранные услуги для формирования
            List<Usl> lstSelUsl = new ArrayList<>();
            if (reqConf.getTp() == 0 && reqConf.getUsl() != null) {
                // начисление по выбранной услуге
                lstSelUsl.add(reqConf.getUsl());
            } else if (reqConf.getTp() == 3) {
                // начисление для распределения по вводу, добавить услуги для ограничения формирования только по ним
                lstSelUsl.add(reqConf.getVvod().getUsl());
            }

            // все действующие счетчики объекта и их объемы
            List<SumMeterVol> lstMeterVol = meterDao.findMeterVolByKlsk(ko.getId(),
                    calcStore.getCurDt1(), calcStore.getCurDt2());
            /*System.out.println("Счетчики:");
            for (SumMeterVol t : lstMeterVol) {
                log.trace("t.getMeterId={}, t.getUslId={}, t.getDtTo={}, t.getDtFrom={}, t.getVol={}",
                        t.getMeterId(), t.getUslId(), t.getDtTo(), t.getDtFrom(), t.getVol());
            }*/
            // получить объемы по счетчикам в пропорции на 1 день их работы
            List<UslMeterDateVol> lstDayMeterVol = meterMng.getPartDayMeterVol(lstMeterVol,
                    calcStore);

            for (UslMeterDateVol t : lstDayMeterVol) {
                log.trace("t.usl={}, t.dt={}, t.vol={}", t.usl.getId(), t.dt, t.vol);
            }

            Calendar c = Calendar.getInstance();

            // получить действующие, отсортированные услуги по помещению (по всем счетам)
            // перенести данный метод внутрь genVolPart, после того как будет реализованна архитектурная возможность
            // отключать услугу в течении месяца
            List<Nabor> lstNabor = naborMng.getValidNabor(ko, null);

            // РАСЧЕТ по блокам:

            // 1. Основные услуги
            // цикл по дням месяца
            int part = 1;
            log.trace("Расчет объемов услуг, до учёта экономии ОДН");
            for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                    .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
                genVolPart(chrgCountAmountLocal, reqConf, parVarCntKpr,
                        parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part, lstNabor);
            }

            // кроме распределения объемов (там нечего еще считать, нет экономии ОДН
            if (reqConf.getTp() != 3) {
                // 2. распределить экономию ОДН по услуге, пропорционально объемам
                log.trace("Распределение экономии ОДН");
                distODNeconomy(chrgCountAmountLocal, ko, lstSelUsl);

                // 3. Зависимые услуги, которые необходимо рассчитать после учета экономии ОДН в основных расчетах
                // цикл по дням месяца (например calcTp=47 - Тепл.энергия для нагрева ХВС или calcTp=19 - Водоотведение)
                part = 2;
                log.trace("Расчет объемов услуг, после учёта экономии ОДН");
                for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                        .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
                    genVolPart(chrgCountAmountLocal, reqConf, parVarCntKpr,
                            parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part, lstNabor);
                }
            }

            // 4. Округлить объемы
            chrgCountAmountLocal.roundVol();

            if (reqConf.getTp() == 3) {
                // 5. Добавить в объемы по вводу
                reqConf.getChrgCountAmount().append(chrgCountAmountLocal);
            }

            chrgCountAmountLocal.printVolAmnt(null, "После округления");

            if (reqConf.getTp() != 3) {
                // 6. Сгруппировать строки начислений для записи в C_CHARGE
                chrgCountAmountLocal.groupUslVolChrg();

                // 7. Умножить объем на цену (расчет в рублях), сохранить в C_CHARGE, округлить для ГИС ЖКХ
                chrgCountAmountLocal.saveChargeAndRound(ko, lstSelUsl);
            }

            log.info("****** {} помещения klskId={}, основной лиц.счет lsk={} - окончание   ******",
                    reqConf.getTpName(), ko.getId());

/*
        log.info("ИТОГО:");
        log.info("UslPriceVolKart:");
        BigDecimal amntVol = BigDecimal.ZERO;

        for (UslPriceVolKart t : calcStore.getChrgCountAmount().getLstUslPriceVolKart()) {
            if (Utl.in(t.usl.getId(),"003")) {
                log.info("dt:{}-{} usl={} org={} cnt={} " +
                                "empt={} stdt={} " +
                                "prc={} prcOv={} prcEm={} " +
                                "vol={} volOv={} volEm={} ar={} arOv={} " +
                                "arEm={} Kpr={} Ot={} Wrz={}",
                        Utl.getStrFromDate(t.dtFrom, "dd"), Utl.getStrFromDate(t.dtTo, "dd"),
                        t.usl.getId(), t.org.getId(), t.isMeter, t.isEmpty,
                        t.socStdt, t.price, t.priceOverSoc, t.priceEmpty,
                        t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.volOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.volEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.areaOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.areaEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.kprOt.setScale(4, BigDecimal.ROUND_HALF_UP),
                        t.kprWr.setScale(4, BigDecimal.ROUND_HALF_UP));
                amntVol=amntVol.add(t.vol.add(t.volOverSoc.add(t.volEmpty)));
            }
        }
        log.info("Итоговый объем:={}", amntVol);
*/
        } finally {
            // разблокировать помещение
            config.getLock().unlockId(reqConf.getRqn(), 1, klskId);
            //log.info("******* klskId={} разблокирован после расчета", klskId);
        }
    }

    /**
     * Расчет объема по услугам
     *
     * @param chrgCountAmountLocal - локальное хранилище объемов, по помещению
     * @param reqConf              - запрос
     * @param parVarCntKpr         - параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
     * @param parCapCalcKprTp      - параметр учета проживающих для капремонта
     * @param ko                   - объект Ko помещения
     * @param lstMeterVol          - объемы по счетчикам
     * @param lstSelUsl            - список услуг для расчета
     * @param lstDayMeterVol       - хранилище объемов по счетчикам
     * @param curDt                - дата расчета
     * @param part                 - группа расчета (услуги рассчитываемые до(1) /после(2) рассчета ОДН
     * @param lstNabor             - список действующих услуг
     * @throws ErrorWhileChrg - ошибка во время расчета
     * @throws WrongParam     - ошибочный параметр
     */
    private void genVolPart(ChrgCountAmountLocal chrgCountAmountLocal,
                            RequestConfigDirect reqConf, int parVarCntKpr,
                            int parCapCalcKprTp, Ko ko, List<SumMeterVol> lstMeterVol, List<Usl> lstSelUsl,
                            List<UslMeterDateVol> lstDayMeterVol, Date curDt, int part, List<Nabor> lstNabor) throws ErrorWhileChrg, WrongParam {

        CalcStore calcStore = reqConf.getCalcStore();
        //boolean isExistsMeterColdWater = false;
        //boolean isExistsMeterHotWater = false;
        //BigDecimal volColdWater = BigDecimal.ZERO;
        //BigDecimal volHotWater = BigDecimal.ZERO;

        // объем по услуге, за рассчитанный день

        Map<Usl, UslPriceVolKart> mapUslPriceVol = new HashMap<>(30);

        for (Nabor nabor : lstNabor) {
            // получить основной лиц счет по связи klsk помещения
            Kart kartMainByKlsk = kartMng.getKartMain(nabor.getKart());
            if (nabor.getUsl().isMain() && (lstSelUsl.size() == 0 || lstSelUsl.contains(nabor.getUsl()))
                    && (part == 1 && !Utl.in(nabor.getUsl().getFkCalcTp(), 47, 19) ||
                    part == 2 && Utl.in(nabor.getUsl().getFkCalcTp(), 47, 19)) // фильтр очередности расчета
                    ) {
                // РАСЧЕТ по основным услугам (из набора услуг или по заданным во вводе)
/*
                log.trace("part={}, {}: lsk={}, uslId={}, fkCalcTp={}, dt={}",
                        part,
                        reqConf.getTpName(),
                        nabor.getKart().getLsk(), nabor.getUsl().getId(),
                        nabor.getUsl().getFkCalcTp(), Utl.getStrFromDate(curDt));
*/
                final Integer fkCalcTp = nabor.getUsl().getFkCalcTp();
                final BigDecimal naborNorm = Utl.nvl(nabor.getNorm(), BigDecimal.ZERO);
                final BigDecimal naborVol = Utl.nvl(nabor.getVol(), BigDecimal.ZERO);
                final BigDecimal naborVolAdd = Utl.nvl(nabor.getVolAdd(), BigDecimal.ZERO);
                // услуга с которой получить объем (иногда выполняется перенаправление, например для fkCalcTp=31)
                final Usl factUslVol = nabor.getUsl().getMeterUslVol() != null ?
                        nabor.getUsl().getMeterUslVol() : nabor.getUsl();
                // ввод
                final Vvod vvod = nabor.getVvod();

                // признаки 0 зарег. и наличия счетчика от связанной услуги
                Boolean isLinkedEmpty = null;
                Boolean isLinkedExistMeter = null;

                // тип распределения ввода
                Integer distTp = 0;
                BigDecimal vvodVol = BigDecimal.ZERO;
                if (vvod != null) {
                    distTp = vvod.getDistTp();
                    vvodVol = Utl.nvl(vvod.getKub(), BigDecimal.ZERO);
                }
                Kart kartMain;
                // получить родительский лиц.счет, если указан явно
                if (Utl.in(nabor.getKart().getTp().getCd(), "LSK_TP_ADDIT", "LSK_TP_RSO")) {
                    // дополнит.счета Капрем., РСО
                    if (nabor.getKart().getParentKart() != null) {
                        kartMain = nabor.getKart().getParentKart();
                    } else {
                        kartMain = kartMainByKlsk;
                    }
                } else {
                    // основные лиц.счета - взять текущий лиц.счет
                    kartMain = nabor.getKart();
                }
                // получить цены по услуге по лицевому счету из набора услуг!
                final DetailUslPrice detailUslPrice = naborMng.getDetailUslPrice(kartMain, nabor);

                CountPers countPers = getCountPersAmount(reqConf, parVarCntKpr, parCapCalcKprTp, curDt, nabor, kartMain);

                SocStandart socStandart = null;
                // получить наличие счетчика
                boolean isMeterExist = false;
                // наличие счетчика х.в.
                boolean isColdMeterExist = false;
                // наличие счетчика г.в.
                boolean isHotMeterExist = false;
                BigDecimal tempVol = BigDecimal.ZERO;
                // объемы
                BigDecimal dayVol = BigDecimal.ZERO;
                // объем по х.в. для водоотведения
                BigDecimal dayColdWaterVol = BigDecimal.ZERO;
                // объем по г.в. для водоотведения
                BigDecimal dayHotWaterVol = BigDecimal.ZERO;
                BigDecimal dayVolOverSoc = BigDecimal.ZERO;

                // площади (взять с текущего лиц.счета)
                final BigDecimal kartArea = Utl.nvl(nabor.getKart().getOpl(), BigDecimal.ZERO);
                BigDecimal areaOverSoc = BigDecimal.ZERO;
                if (Utl.in(fkCalcTp, 25) // Текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                        || fkCalcTp.equals(7) && nabor.getKart().getStatus().getId().equals(1) // Найм (только по муниципальным помещениям) расчет на м2
                        // fixme: по Найму скорее всего ошибка, так как надо смотреть в kartMain - исправить, когда перейдём на новую версию начисления
                        || (fkCalcTp.equals(24) || fkCalcTp.equals(32) // Прочие услуги, расчитываемые как расценка * норматив * общ.площадь
                        && !nabor.getKart().getStatus().getId().equals(1))// или 32 услуга, только не по муниципальному фонду
                        || fkCalcTp.equals(36)// Вывоз жидких нечистот и т.п. услуги
                        || fkCalcTp.equals(37) && !countPers.isSingleOwnerOlder70// Капремонт и если не одинокие пенсионеры старше 70, кроме муницип.помещений
                        && !nabor.getKart().getStatus().getId().equals(1)
                        ) {
                    if (Utl.in(fkCalcTp, 25)) {
                        // Текущее содержание - получить соц.норму
                        socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                    }
                    dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 17, 18, 31)) {
                    // Х.В., Г.В., без уровня соцнормы/свыше, электроэнергия
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне
                    //log.info("factUslVol.getId()={}", factUslVol.getId());
/*
                    for (SumMeterVol t : lstMeterVol) {
                        log.info("$$$$$$ t.getVol()={}, t.getDtFrom()={}, t.getDtTo()={}, t.getUslId()={}, t.getMeterId()={}" +
                                "", t.getVol(), t.getDtFrom(), t.getDtTo(), t.getUslId(), t.getMeterId());
                    }
*/
                    isMeterExist = meterMng.isExistAnyMeter(lstMeterVol, factUslVol.getId(), curDt);
                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                    if (isMeterExist) {
                        // для водоотведения
                       /* if (fkCalcTp.equals(17)) {
                            isExistsMeterColdWater = true;
                        } else if (fkCalcTp.equals(18)) {
                            isExistsMeterHotWater = true;
                        }*/
                        // получить объем по счетчику в пропорции на 1 день его работы
                        UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                .findFirst().orElse(null);
                        if (partVolMeter != null) {
                            tempVol = partVolMeter.vol;
                        }
                    } else {
                        // норматив в пропорции на 1 день месяца
                        tempVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                    }

                    dayVol = tempVol;
                    //area = kartArea;

                    // для водоотведения и Х.В.для ГВС
/*
                    if (fkCalcTp.equals(17)) {
                        volColdWater = tempVol;
                    } else if (fkCalcTp.equals(18)) {
                        volHotWater = tempVol;
                    }
*/
                } else if (Utl.in(fkCalcTp, 19)) {
                    // Водоотведение без уровня соцнормы/свыше
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне

                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(nabor, countPers);

/*
                    if (isExistsMeterColdWater || isExistsMeterHotWater) {
                        isMeterExist = true;
                    }
*/

                    List<UslPriceVolKart> lstColdHotWater =
                            chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                    .filter(t -> t.dt.equals(curDt)
                                            && t.kart.getKoKw().equals(nabor.getKart().getKoKw())
                                            && Utl.in(t.usl.getFkCalcTp(), 17, 18)).collect(Collectors.toList());
                    // сложить предварительно рассчитанные объемы х.в.+г.в., найти признаки наличия счетчиков
                    for (UslPriceVolKart t : lstColdHotWater) {
                        if (t.usl.getFkCalcTp().equals(17)) {
                            // х.в.
                            dayColdWaterVol = dayVol.add(t.vol);
                            isColdMeterExist = t.isMeter;
                        } else {
                            // г.в.
                            dayHotWaterVol = dayHotWaterVol.add(t.vol);
                            isHotMeterExist = t.isMeter;
                        }
                    }
                } else if (Utl.in(fkCalcTp, 14)) {
                    // Отопление гкал. без уровня соцнормы/свыше
                    if (Utl.nvl(kartMain.getPot(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) != 0) {
                        // есть показания по Индивидуальному счетчику отопления (Бред!)
                        tempVol = Utl.nvl(kartMain.getMot(), BigDecimal.ZERO);
                    } else {
                        if (kartArea.compareTo(BigDecimal.ZERO) != 0) {
                            if (distTp.equals(1)) {
                                // есть ОДПУ по отоплению гкал, начислить по распределению
                                tempVol = naborVol;
                            } else if (Utl.in(distTp, 4, 5)) {
                                // нет ОДПУ по отоплению гкал, начислить по нормативу с учётом отопительного сезона
                                if (vvod.getIsChargeInNotHeatingPeriod()) {
                                    // начислять и в НЕотопительном периоде
                                    tempVol = kartArea.multiply(naborNorm);
                                } else {
                                    // начислять только в отопительном периоде
                                    if (Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT3"),
                                            sprParamMng.getD1("MONTH_HEAT4"))) {
                                        tempVol = kartArea.multiply(naborNorm);
                                    }
                                }
                            }
                        }

                    }
                    //  в доле на 1 день
                    // помещение с проживающими
                    dayVol = tempVol.multiply(calcStore.getPartDayMonth());
                    //log.info("************************ dayVol={}", dayVol);
                    //area = kartArea;
                } else if (Utl.in(fkCalcTp, 12)) {
                    // Антенна, код.замок
                    //area = kartArea;
                    dayVol = calcStore.getPartDayMonth();
                } else if (Utl.in(fkCalcTp, 20, 21, 23)) {
                    // Х.В., Г.В., Эл.Эн. содерж.общ.им.МКД, Эл.эн.гараж
                    //area = kartArea;
                    dayVol = naborVolAdd.multiply(calcStore.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 34, 44)) {
                    // Повыш.коэфф
                    if (nabor.getUsl().getParentUsl() != null) {
                        // получить объем из родительской услуги
                        UslPriceVolKart uslPriceVolKart = mapUslPriceVol.get(nabor.getUsl().getParentUsl());
                        if (uslPriceVolKart != null && !uslPriceVolKart.isMeter) {
                            // только если нет счетчика в родительской услуге
                            //area = kartArea;
                            // сложить все объемы родит.услуги, умножить на норматив текущей услуги
                            dayVol = (uslPriceVolKart.vol.add(uslPriceVolKart.volOverSoc))
                                    .multiply(naborNorm);
                        }

                    } else {
                        throw new ErrorWhileChrg("ОШИБКА! По услуге usl.id=" + nabor.getUsl().getId() +
                                " отсутствует PARENT_USL");
                    }
                } else if (fkCalcTp.equals(49)) {
                    // Вывоз мусора - кол-во прожив * цену (Кис.)
                    //area = kartArea;
                    dayVol = BigDecimal.valueOf(countPers.kprNorm).multiply(calcStore.getPartDayMonth());
                } else if (fkCalcTp.equals(47)) {
                    // Тепл.энергия для нагрева ХВС (Кис.)
                    //area = kartArea;
                    Usl uslLinked = uslDao.getByCd("х.в. для гвс");
                    BigDecimal vvodVol2 = ko.getKart().stream()
                            .flatMap(t -> t.getNabor().stream())
                            .filter(t -> t.getUsl().equals(uslLinked))
                            .map(t -> t.getVvod().getKub())
                            .findFirst().orElse(BigDecimal.ZERO);

                    // получить объем по расчетному дню связанной услуги
                    UslPriceVolKart uslPriceVolKart = chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                            .filter(t -> t.dt.equals(curDt)
                                    && t.kart.getKoKw().equals(nabor.getKart().getKoKw())
                                    && t.usl.equals(uslLinked))
                            .findFirst().orElse(null);

                    if (uslPriceVolKart != null) {
                        isLinkedEmpty = uslPriceVolKart.isEmpty;
                        isLinkedExistMeter = uslPriceVolKart.isMeter;
                        if (vvodVol2.compareTo(BigDecimal.ZERO) != 0) {
                            dayVol = uslPriceVolKart.vol.divide(vvodVol2, 20, BigDecimal.ROUND_HALF_UP)
                                    .multiply(vvodVol);
                        }
                    }
                } else if (fkCalcTp.equals(6) && countPers.kpr > 0) {
                    // Очистка выгр.ям (Полыс.) (при наличии проживающих)
                    // просто взять цену
                    //area = kartArea;
                    dayVol = calcStore.getPartDayMonth();
/*                    } else { TODO Вернуть в конце разработки!
                    throw new ErrorWhileChrg("ОШИБКА! По услуге fkCalcTp=" + fkCalcTp +
                            " не определён блок if в GenChrgProcessMngImpl.genChrg");
*/
                }

                UslPriceVolKart uslPriceVolKart = null;
                if (nabor.getUsl().getFkCalcTp().equals(19)) {
                    // водоотведение, добавить составляющие по х.в. и г.в.
                    if (dayColdWaterVol.compareTo(BigDecimal.ZERO) != 0) {
                        uslPriceVolKart = buildVol(curDt, calcStore, nabor, null, null,
                                kartMain, detailUslPrice, countPers, socStandart, isColdMeterExist,
                                dayColdWaterVol, dayVolOverSoc, kartArea, areaOverSoc);
                        // сгруппировать по лиц.счету, услуге, для распределения по вводу
                        chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                    }
                    if (dayHotWaterVol.compareTo(BigDecimal.ZERO) != 0) {
                        uslPriceVolKart = buildVol(curDt, calcStore, nabor, null, null,
                                kartMain, detailUslPrice, countPers, socStandart, isHotMeterExist,
                                dayHotWaterVol, dayVolOverSoc, kartArea, areaOverSoc);
                        // сгруппировать по лиц.счету, услуге, для распределения по вводу
                        chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                    }
                } else {
                    // прочие услуги
                    uslPriceVolKart = buildVol(curDt, calcStore, nabor, isLinkedEmpty, isLinkedExistMeter,
                            kartMain, detailUslPrice, countPers, socStandart, isMeterExist,
                            dayVol, dayVolOverSoc, kartArea, areaOverSoc);
                    if (Utl.in(nabor.getUsl().getFkCalcTp(), 17, 18)) {
                        // по х.в., г.в.
                        // сохранить расчитанный объем по расчетному дню, (используется для услуги Повыш коэфф.)
                        mapUslPriceVol.put(nabor.getUsl(), uslPriceVolKart);
                    }
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                }


                //                        log.info("******* RESID={}", uslPriceVolKart.isResidental);

                    /*
                    if (Utl.in(uslPriceVolKart.usl.getId(),"003")) {
                        log.info("РАСЧЕТ ДНЯ:");
                        log.info("dt:{}-{} usl={} org={} cnt={} " +
                                        "empt={} stdt={} " +
                                        "prc={} prcOv={} prcEm={} " +
                                        "vol={} volOv={} volEm={} ar={} arOv={} " +
                                        "arEm={} Kpr={} Ot={} Wrz={}",
                                Utl.getStrFromDate(uslPriceVolKart.dtFrom, "dd"), Utl.getStrFromDate(uslPriceVolKart.dtTo, "dd"),
                                uslPriceVolKart.usl.getId(), uslPriceVolKart.org.getId(), uslPriceVolKart.isMeter, uslPriceVolKart.isEmpty,
                                uslPriceVolKart.socStdt, uslPriceVolKart.price, uslPriceVolKart.priceOverSoc, uslPriceVolKart.priceEmpty,
                                uslPriceVolKart.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.volOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.volEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.area.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.areaOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.areaEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.kpr.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.kprOt.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.kprWr.setScale(4, BigDecimal.ROUND_HALF_UP));
                    }
*/

/*
                if (lstSelUsl.size() == 0 && nabor.getUsl().getId().equals("015") || nabor.getUsl().getId().equals("099")) {
                    log.info("************!!!!!!!! usl={}, vol={}, dt={}", nabor.getUsl().getId(), dayVol, Utl.getStrFromDate(curDt));
                }
                if (lstSelUsl.size() == 0 && nabor.getUsl().getId().equals("015") || nabor.getUsl().getId().equals("099")) {
                    for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
                        if (t.usl.getId().equals("015") || t.usl.getId().equals("099")) {
                            log.info("***********!!!!!! dt={}, lsk={}, usl={} vol={} ar={} Kpr={}",
                                    Utl.getStrFromDate(curDt), t.kart.getLsk(), t.usl.getId(),
                                    t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
                        }
                    }
                }
*/

            }
        }

    }

    /**
     * Построить объем для начисления
     * @param curDt - дата расчета
     * @param calcStore - хранилище объемов
     * @param nabor - строка набора
     * @param isLinkedEmpty -
     * @param isLinkedExistMeter -
     * @param kartMain - лиц.счет
     * @param detailUslPrice - инф. о расценке
     * @param countPers - инф. о кол.прожив.
     * @param socStandart - соцнорма
     * @param isMeterExist - наличие счетчика
     * @param dayVol - объем
     * @param dayVolOverSoc - объем свыше соц.нормы
     * @param kartArea - площадь
     * @param areaOverSoc - площадь свыше соц.нормы
     */
    private UslPriceVolKart buildVol(Date curDt, CalcStore calcStore, Nabor nabor, Boolean isLinkedEmpty,
                                     Boolean isLinkedExistMeter, Kart kartMain, DetailUslPrice detailUslPrice,
                                     CountPers countPers, SocStandart socStandart, boolean isMeterExist,
                                     BigDecimal dayVol, BigDecimal dayVolOverSoc, BigDecimal kartArea, BigDecimal areaOverSoc) {
        return UslPriceVolKart.UslPriceVolBuilder.anUslPriceVol()
                .withDt(curDt)
                .withKart(nabor.getKart()) // группировать по лиц.счету из nabor!
                .withUsl(nabor.getUsl())
                .withUslOverSoc(detailUslPrice.uslOverSoc)
                .withUslEmpt(detailUslPrice.uslEmpt)
                .withOrg(nabor.getOrg())
                .withIsMeter(isLinkedExistMeter != null ? isLinkedExistMeter : isMeterExist)
                .withIsEmpty(isLinkedEmpty != null ? isLinkedEmpty : countPers.isEmpty)
                .withIsResidental(kartMain.isResidental())
                .withSocStdt(socStandart != null ? socStandart.norm : BigDecimal.ZERO)
                .withPrice(detailUslPrice.price)
                .withPriceOverSoc(detailUslPrice.priceOverSoc)
                .withPriceEmpty(detailUslPrice.priceEmpt)
                .withVol(dayVol)
                .withVolOverSoc(dayVolOverSoc)
                .withArea(kartArea)
                .withAreaOverSoc(areaOverSoc)
                .withKpr(countPers.kpr)
                .withKprOt(countPers.kprOt)
                .withKprWr(countPers.kprWr)
                .withKprMax(countPers.kprMax)
                .withPartDayMonth(calcStore.getPartDayMonth())
                .build();
    }

    /**
     * Получить совокупное кол-во проживающих (родительский и дочерний лиц.счета)
     *
     * @param parVarCntKpr    - тип подсчета кол-во проживающих
     * @param parCapCalcKprTp - тип подсчета кол-во проживающих для капремонта
     * @param curDt           - дата расчета
     * @param nabor           - строка услуги
     * @param kartMain        - основной лиц.счет
     */
    private CountPers getCountPersAmount(RequestConfigDirect reqConf, int parVarCntKpr, int parCapCalcKprTp,
                                         Date curDt, Nabor nabor, Kart kartMain) {
        CountPers countPers;
        countPers = kartPrMng.getCountPersByDate(kartMain, nabor,
                parVarCntKpr, parCapCalcKprTp, curDt);

        if (nabor.getKart().getParentKart() != null) {
            // в дочернем лиц.счете
            // для определения расценки по родительскому (если указан по parentKart) или текущему лиц.счету
            CountPers countPersParent = kartPrMng.getCountPersByDate(nabor.getKart().getParentKart(), nabor,
                    parVarCntKpr, parCapCalcKprTp, curDt);
            countPers.kpr = countPersParent.kpr;
            countPers.isEmpty = countPersParent.isEmpty;

            // алгоритм взят из C_KART, строка 786
            if (parVarCntKpr == 0 &&
                    (reqConf.getGenDt().getTime() > Utl.getDateFromStr("01.02.2019").getTime() // после 01.02.19 - не учитывать тип счетов
                            || Utl.in(nabor.getKart().getTp().getCd(), "LSK_TP_RSO"))
                    && countPers.kprNorm == 0
                    && countPers.kprOt == 0 && !kartMain.getStatus().getCd().equals("MUN")) {
                // вариант Кис.
                // в РСО счетах и кол-во временно отсут.=0
                countPers.kprNorm = 1;
            }
        } else {
            // в родительском лиц.счете
            if (countPers.kprNorm == 0) {
                if (parVarCntKpr == 0) {
                    // Киселёвск
                    if (!kartMain.getStatus().getCd().equals("MUN")) {
                        // не муницип. помещение
                        if (nabor.getUsl().getFkCalcTp().equals(49)) {
                            // услуга по обращению с ТКО
                            countPers.kpr = 1;
                            countPers.kprNorm = 1;
                        } else if (countPers.kprOt == 0) {
                            countPers.kprNorm = 1;
                        }
                    }
                } else if (parVarCntKpr == 1 && countPers.kprOt == 0) {
                    // Полысаево
                    countPers.kprNorm = 1;
                }
            }
        }
        countPers.isEmpty = countPers.kpr == 0;
        return countPers;
    }

    /**
     * Распределить объемы экономии по услугам лиц.счетов, рассчитанных по помещению
     *
     * @param chrgCountAmountLocal - локальное хранилище объемов по помещению
     * @param ko                   - помещение
     * @param lstSelUsl            - список ограничения услуг (например при распределении ОДН)
     */
    private synchronized void distODNeconomy(ChrgCountAmountLocal chrgCountAmountLocal,
                                             Ko ko, List<Usl> lstSelUsl) throws ErrorWhileChrg {
        // получить объемы экономии по всем лиц.счетам помещения
        List<ChargePrep> lstChargePrep = ko.getKart().stream()
                .flatMap(t -> t.getChargePrep().stream())
                .filter(c -> c.getTp().equals(4) && lstSelUsl.size() == 0)
                .filter(c -> c.getKart().getNabor().stream()
                        .anyMatch(n -> n.getUsl().equals(c.getUsl().getUslChild())
                                && n.isValid())) // только по действительным услугам ОДН
                .collect(Collectors.toList());

        // распределить экономию
        for (ChargePrep t : lstChargePrep) {
            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема в лиц.счете (когда были проживающие)
            List<UslVolKart> lstUslVolKart = chrgCountAmountLocal.getLstUslVolKart().stream()
                    .filter(d -> d.kart.equals(t.getKart()) && d.kpr.compareTo(BigDecimal.ZERO) != 0 && d.usl.equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета
            Utl.distBigDecimalByList(t.getVol(), lstUslVolKart, 5);

            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема во вводе (когда были проживающие)
            List<UslVolVvod> lstUslVolVvod = chrgCountAmountLocal.getLstUslVolVvod().stream()
                    .filter(d -> d.kpr.compareTo(BigDecimal.ZERO) != 0 && d.usl.equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов ввода
            Utl.distBigDecimalByList(t.getVol(), lstUslVolVvod, 5);

            // РАСПРЕДЕЛИТЬ по датам, детально
            // в т.ч. для услуги calcTp=47 (Тепл.энергия для нагрева ХВС (Кис.)) (когда были проживающие)
            // в т.ч. по услугам х.в. и г.в. (для водоотведения)
            List<UslPriceVolKart> lstUslPriceVolKart = chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                    .filter(d -> d.kart.equals(t.getKart()) && d.kpr.compareTo(BigDecimal.ZERO) != 0 && d.usl.equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета, по датам
            Utl.distBigDecimalByList(t.getVol(), lstUslPriceVolKart, 5);

            // ПО СГРУППИРОВАННЫМ объемам до лиц.счетов, просто снять объем
            UslVolKartGrp uslVolKartGrp = chrgCountAmountLocal.getLstUslVolKartGrp().stream()
                    .filter(d -> d.kart.equals(t.getKart()) && d.usl.equals(t.getUsl()))
                    .findFirst().orElse(null);
            if (uslVolKartGrp != null) {
                uslVolKartGrp.vol = uslVolKartGrp.vol.add(t.getVol());
            }
        }
    }
}