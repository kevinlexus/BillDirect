package com.dic.app.mm.impl;

import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.RequestConfig;
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
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

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
@Scope("prototype")
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
    private SprParamMng sprParamMng;
    @PersistenceContext
    private EntityManager em;

    /**
     * Рассчитать начисление
     * Внимание! Расчет идёт по квартире (помещению), но информация группируется по лиц.счету(Kart)
     * так как теоретически может быть одинаковая услуга на разных лиц.счетах, но на одной квартире!
     *
     * @param calcStore - хранилище справочников
     * @param ko        - Ko квартиры
     * @param reqConf   - конфиг запроса
     */
    @Override
    public void genChrg(CalcStore calcStore, Ko ko, RequestConfig reqConf) throws WrongParam, ErrorWhileChrg {
        // получить основной лиц счет по связи klsk квартиры
        Kart kartMainByKlsk = kartMng.getKartMain(ko);
        log.info("****** Расчет квартиры klskId={} ****** Основной лиц.счет lsk={}", ko.getId(), kartMainByKlsk.getLsk());
        // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
        int parVarCntKpr =
                Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"), 0D).intValue();
        // параметр учета проживающих для капремонта
        int parCapCalcKprTp =
                Utl.nvl(sprParamMng.getN1("CAP_CALC_KPR_TP"), 0D).intValue();

        //ChrgCount chrgCount = new ChrgCount();
        // выбранные услуги для формирования
        List<Usl> lstSelUsl = new ArrayList<>();
        if (reqConf.getTp() == 2) {
            // распределение по вводу, добавить услуги для ограничения формирования только по ним
            lstSelUsl.add(reqConf.getVvod().getUsl());
        }

        // все действующие счетчики объекта и их объемы
        List<SumMeterVol> lstMeterVol = meterDao.findMeterVolByKlsk(ko.getId(),
                calcStore.getCurDt1(), calcStore.getCurDt2());

        // сохранить помещение
        //chrgCount.setKo(ko);
        // получить все действующие счетчики квартиры и их объемы
        //chrgCount.setLstMeterVol();
        // получить объемы по счетчикам в пропорции на 1 день их работы
        List<UslMeterDateVol> lstDayMeterVol = meterMng.getPartDayMeterVol(lstMeterVol,
                calcStore);

        Calendar c = Calendar.getInstance();
        // РАСЧЕТ по блокам:

        // 1. ОСНОВНЫЕ услуги
        // цикл по дням месяца
        int part = 1;
        log.info("Распределение объемов для ОДН");
        for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
            genVolPart(calcStore, reqConf, kartMainByKlsk, parVarCntKpr,
                    parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part);
        }

        // 2. распределить экономию ОДН по услуге, пропорционально объемам
        log.info("Распределение экономии ОДН");
        distODNeconomy(calcStore, ko, lstSelUsl);

        // 3. ЗАВИСИМЫЕ услуги, которые необходимо рассчитать после учета экономии ОДН в основных расчетах
        // цикл по дням месяца (например calcTp=47 - Тепл.энергия для нагрева ХВС)
        part = 2;
        log.info("Рассчет начисления");
        for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
            genVolPart(calcStore, reqConf, kartMainByKlsk, parVarCntKpr,
                    parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part);
        }

        // 4. ОКРУГЛИТЬ объемы
        calcStore.getChrgCountAmount().roundVol();

        // 5. УМНОЖИТЬ объем на цену (расчет в рублях)

        // сохранить в объемы дома (для расчета ОДН и прочих распределений во вводах)
        //calcStore.getChrgCountAmount().addChrgCount(chrgCount);

        // получить кол-во проживающих по лиц.счету
/*
        log.info("ИТОГО:");
        log.info("UslPriceVolKart:");
        BigDecimal amntVol = BigDecimal.ZERO;

        for (UslPriceVolKart t : chrgCount.getLstUslPriceVolKart()) {
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
    }

    /**
     * Расчет объема по услугам
     * @param calcStore - хранилище справочников
     * @param reqConf - запрос
     * @param kartMainByKlsk - основной лиц.счет
     * @param parVarCntKpr - параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
     * @param parCapCalcKprTp - параметр учета проживающих для капремонта
     * @param ko - объект Ko квартиры
     * @param lstMeterVol - объемы по счетчикам
     * @param lstSelUsl - список услуг для расчета
     * @param lstDayMeterVol - хранилище объемов по счетчикам
     * @param curDt - дата расчета
     * @param part - группа расчета (услуги рассчитываемые до(1) /после(2) рассчета ОДН
     * @throws ErrorWhileChrg - ошибка во время расчета
     * @throws WrongParam - ошибочный параметр
     */
    private void genVolPart(CalcStore calcStore, RequestConfig reqConf, Kart kartMainByKlsk, int parVarCntKpr,
                            int parCapCalcKprTp, Ko ko, List<SumMeterVol> lstMeterVol, List<Usl> lstSelUsl,
                            List<UslMeterDateVol> lstDayMeterVol, Date curDt, int part) throws ErrorWhileChrg, WrongParam {
        // получить действующие, отсортированные услуги по квартире (по всем счетам)
        List<Nabor> lstNabor = naborMng.getValidNabor(ko, curDt);

        boolean isExistsMeterColdWater = false;
        boolean isExistsMeterHotWater = false;
        BigDecimal volColdWater = BigDecimal.ZERO;
        BigDecimal volHotWater = BigDecimal.ZERO;

        // объем по услуге, за рассчитанный день

        Map<Usl, UslPriceVolKart> mapUslPriceVol = new HashMap<>(30);

        for (Nabor nabor : lstNabor) {
            if (nabor.getUsl().isMain() && (lstSelUsl.size() == 0 || lstSelUsl.contains(nabor.getUsl()))
                    && (part == 1 && !nabor.getUsl().getFkCalcTp().equals(47) ||
                        part == 2 && nabor.getUsl().getFkCalcTp().equals(47)) // фильтр очередности расчета
                    ) {
                // РАСЧЕТ по основным услугам (из набора услуг или по заданным во вводе)
                log.trace("{}: lsk={}, uslId={}, fkCalcTp={}, dt={}",
                        reqConf.getTpName(),
                        nabor.getKart().getLsk(), nabor.getUsl().getId(),
                        nabor.getUsl().getFkCalcTp(), Utl.getStrFromDate(curDt));
                final Integer fkCalcTp = nabor.getUsl().getFkCalcTp();
                final BigDecimal naborNorm = Utl.nvl(nabor.getNorm(), BigDecimal.ZERO);
                final BigDecimal naborVol = Utl.nvl(nabor.getVol(), BigDecimal.ZERO);
                final BigDecimal naborVolAdd = Utl.nvl(nabor.getVolAdd(), BigDecimal.ZERO);
                // услуга с которой получить объем (иногда выполняется перенаправление, например для fkCalcTp=31)
                final Usl factUslVol = nabor.getUsl().getFactUslVol() != null ?
                        nabor.getUsl().getFactUslVol() : nabor.getUsl();
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
                // получить основной лиц.счет, если указан явно
                if (nabor.getKart().getParentKart() != null) {
                    kartMain = nabor.getKart();
                } else {
                    kartMain = kartMainByKlsk;
                }
                // получить цены по услуге по лицевому счету из набора услуг!
                final DetailUslPrice detailUslPrice = naborMng.getDetailUslPrice(kartMain, nabor);

                // получить кол-во проживающих по лицевому счету из набора услуг!
                final CountPers countPers = kartPrMng.getCountPersByDate(kartMain, nabor,
                        parVarCntKpr, parCapCalcKprTp, curDt);
                SocStandart socStandart = null;
                // получить наличие счетчика
                boolean isMeterExist = false;
                BigDecimal tempVol = BigDecimal.ZERO;
                // объемы
                BigDecimal dayVol = BigDecimal.ZERO;
                BigDecimal dayVolOverSoc = BigDecimal.ZERO;

                // площади
                final BigDecimal kartArea = Utl.nvl(kartMain.getOpl(), BigDecimal.ZERO);

                BigDecimal area = BigDecimal.ZERO;
                BigDecimal areaOverSoc = BigDecimal.ZERO;

                if (Utl.in(fkCalcTp, 25)) {
                    // Текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                    area = kartArea;
                    dayVol = getRoundedVolByDate(calcStore, area, curDt);
                    socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                } else if (Utl.in(fkCalcTp, 17, 18, 31)) {
                    // Х.В., Г.В., без уровня соцнормы/свыше, электроэнергия
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне
                    isMeterExist = meterMng.isExistAnyMeter(lstMeterVol, factUslVol.getId(), curDt);
                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                    if (isMeterExist) {
                        // для водоотведения
                        if (fkCalcTp.equals(17)) {
                            isExistsMeterColdWater = true;
                        } else if (fkCalcTp.equals(18)) {
                            isExistsMeterHotWater = true;
                        }
                        // получить объем по счетчику в пропорции на 1 день его работы
                        UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                .filter(t -> t.usl.equals(nabor.getUsl()) && t.dt.equals(curDt))
                                .findFirst().orElse(null);
                        if (partVolMeter != null) {
                            tempVol = partVolMeter.vol;
                        }
                    } else {
                        // норматив в пропорции на 1 день месяца
                        tempVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                    }

                    dayVol = tempVol;
                    area = kartArea;

                    // для водоотведения и Х.В.для ГВС
                    if (fkCalcTp.equals(17)) {
                        volColdWater = tempVol;
                    } else if (fkCalcTp.equals(18)) {
                        volHotWater = tempVol;
                    }
                } else if (Utl.in(fkCalcTp, 19)) {
                    // Водоотведение без уровня соцнормы/свыше
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне

                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(nabor, countPers);

                    if (isExistsMeterColdWater || isExistsMeterHotWater) {
                        isMeterExist = true;
                    }
                    // сложить предварительно рассчитанные объемы х.в.+г.в.
                    tempVol = volColdWater.add(volHotWater);
                    // квартира с проживающими
                    dayVol = tempVol;
                    area = kartArea;

                } else if (Utl.in(fkCalcTp, 14)) {
                    // Отопление гкал. без уровня соцнормы/свыше
                    if (!Utl.nvl(kartMain.getPot(), BigDecimal.ZERO).equals(BigDecimal.ZERO)) {
                        // есть показания по Индивидуальному счетчику отопления (Бред!)
                        dayVol = getRoundedVolByDate(calcStore, Utl.nvl(kartMain.getMot(), BigDecimal.ZERO), curDt);
                    } else {
                        if (!kartArea.equals(BigDecimal.ZERO)) {
                            if (distTp.equals(1)) {
                                // есть ОДПУ по отоплению гкал, начислить по распределению
                                dayVol = getRoundedVolByDate(calcStore, naborVol, curDt);
                            } else if (Utl.in(distTp, 4, 5)) {
                                // нет ОДПУ по отоплению гкал, начислить по нормативу с учётом отопительного сезона
                                if (vvod.getIsChargeInNotHeatingPeriod()) {
                                    // начислять и в НЕотопительном периоде
                                    dayVol = getRoundedVolByDate(calcStore, kartArea.multiply(naborNorm), curDt);
                                } else {
                                    // начислять только в отопительном периоде
                                    if (Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT3"),
                                            sprParamMng.getD1("MONTH_HEAT4"))) {
                                        dayVol = getRoundedVolByDate(calcStore, kartArea.multiply(naborNorm), curDt);;
                                    }
                                }
                            }
                        }

                    }
                    area = kartArea;
                } else if (fkCalcTp.equals(7) && kartMain.getStatus().getId().equals(1)) {
                    // Найм (только по муниципальным квартирам) расчет на м2
                    area = kartArea;
                    dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 12)) {
                    // Антенна, код.замок
                    area = kartArea;
                    dayVol = calcStore.getPartDayMonth();
                } else if (Utl.in(fkCalcTp, 20, 21, 23)) {
                    // Х.В., Г.В., Эл.Эн. содерж.общ.им.МКД, Эл.эн.гараж
                    area = kartArea;
                    dayVol = naborVolAdd.multiply(calcStore.getPartDayMonth());
                } else if (fkCalcTp.equals(24) || fkCalcTp.equals(32) && !kartMain.getStatus().getId().equals(1)) {
                    // Прочие услуги, расчитываемые как расценка * норматив * общ.площадь
                    // или 32 услуга, только не по муниципальному фонду
                    area = kartArea;
                    dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                } else if (fkCalcTp.equals(36)) {
                    // Вывоз жидких нечистот и т.п. услуги
                    area = kartArea;
                    dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                } else if (fkCalcTp.equals(37) && !countPers.isSingleOwnerOlder70) {
                    // Капремонт и если не одинокие пенсионеры старше 70
                    area = kartArea;
                    dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 34, 44)) {
                    // Повыш.коэфф
                    if (nabor.getUsl().getParentUsl() != null) {
                        // получить объем из родительской услуги
                        UslPriceVolKart uslPriceVolKart = mapUslPriceVol.get(nabor.getUsl().getParentUsl());
                        if (uslPriceVolKart != null && !uslPriceVolKart.isMeter) {
                            // только если нет счетчика в родительской услуге
                            area = kartArea;
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
                    area = kartArea;
                    dayVol = BigDecimal.valueOf(countPers.kpr).multiply(calcStore.getPartDayMonth());
                } else if (fkCalcTp.equals(47)) {
                    // Тепл.энергия для нагрева ХВС (Кис.)
                    area = kartArea;
                    Usl uslLinked = uslDao.getByCd("х.в. для гвс");
                    BigDecimal vvodVol2 = ko.getKart().stream()
                            .flatMap(t -> t.getNabor().stream())
                            .filter(t -> t.getUsl().equals(uslLinked))
                            .map(t -> t.getVvod().getKub())
                            .findFirst().orElse(BigDecimal.ZERO);

                    // получить объем по расчетному дню связанной услуги
                    UslPriceVolKart uslPriceVolKart = calcStore.getChrgCountAmount().getLstUslPriceVolKart().stream()
                            .filter(t -> t.dt.equals(curDt)
                                    && t.kart.getKoKw().equals(nabor.getKart().getKoKw())
                                    && t.usl.equals(uslLinked))
                            .findFirst().orElse(null);

                    if (uslPriceVolKart != null) {
                        isLinkedEmpty = uslPriceVolKart.isEmpty;
                        isLinkedExistMeter = uslPriceVolKart.isMeter;
                        if (!vvodVol2.equals(BigDecimal.ZERO)) {
                            dayVol = uslPriceVolKart.vol.divide(vvodVol2, 20, BigDecimal.ROUND_HALF_UP)
                                    .multiply(vvodVol);
/*
                            if (nabor.getKart().getLsk().equals("РСО_0001")) {
                                log.info("!!!!!!!!!!!!!! lsk={}, dt={}, uslPriceVolKart.vol={}, vvvodVol2={}, vvodVol={}, " +
                                                "dayVol={}",
                                        nabor.getKart().getLsk(), Utl.getStrFromDate(curDt), uslPriceVolKart.vol, vvodVol2, vvodVol,
                                        dayVol);
                            }
*/
                        }
                    }


                } else if (fkCalcTp.equals(6) && countPers.kpr > 0) {
                    // Очистка выгр.ям (Полыс.) (при наличии проживающих)
                    // просто взять цену
                    area = kartArea;
                    dayVol = calcStore.getPartDayMonth();
/*                    } else { TODO Вернуть в конце разработки!
                    throw new ErrorWhileChrg("ОШИБКА! По услуге fkCalcTp=" + fkCalcTp +
                            " не определён блок if в GenChrgProcessMngImpl.genChrg");
*/
                }

                // сгруппировать, если есть объемы
                //if (!dayVol.equals(BigDecimal.ZERO) || !dayVolOverSoc.equals(BigDecimal.ZERO)) { нужны нулевые объемы!
                UslPriceVolKart uslPriceVolKart = UslPriceVolKart.UslPriceVolBuilder.anUslPriceVol()
                        .withDt(curDt)
                        .withKart(nabor.getKart()) // группировать по лиц.счету из nabor!
                        .withUsl(nabor.getUsl())
                        .withOrg(nabor.getOrg())
                        .withIsCounter(isLinkedExistMeter != null? isLinkedExistMeter : isMeterExist)
                        .withIsEmpty(isLinkedEmpty != null? isLinkedEmpty : countPers.isEmpty)
                        .withIsResidental(kartMain.isResidental())
                        .withSocStdt(socStandart != null ? socStandart.norm : BigDecimal.ZERO)
                        .withPrice(detailUslPrice.price)
                        .withPriceOverSoc(detailUslPrice.priceOverSoc)
                        .withPriceEmpty(detailUslPrice.priceEmpt)
                        .withVol(dayVol)
                        .withVolOverSoc(dayVolOverSoc)
                        .withArea(area) // note Сделать, чтобы бралось по getRoundedVolByDate(
                        .withAreaOverSoc(areaOverSoc)
                        .withKpr(countPers.kpr).withKprOt(countPers.kprOt).withKprWr(countPers.kprWr)
                        .withPartDayMonth(calcStore.getPartDayMonth())
                        .build();

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
                // сохранить рассчитанный объем по расчетному дню, (используется для услуги Повыш коэфф.)
                mapUslPriceVol.put(nabor.getUsl(), uslPriceVolKart);

                // note исключить ненужные услуги из добавления в группировку по дням, иначе будет долго работать
                // сгруппировать по лиц.счету, услуге, для распределения по вводу
                calcStore.getChrgCountAmount().groupUslVol(uslPriceVolKart);

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
     * Получить округленную долю объема в зависимости от даты
     * @param calcStore - хранилище объемов
     * @param vol - объем к распределению
     * @param dt - дата расчета
     * @return
     */
    private BigDecimal getRoundedVolByDate(CalcStore calcStore, BigDecimal vol, Date dt) {
        Calendar c = Calendar.getInstance();
        BigDecimal diff = vol;
        Date lastPartVolDt = null;
        Map<Date, BigDecimal> map = new HashMap<>();
        for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2());
             c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();
            BigDecimal partVol = vol.multiply(calcStore.getPartDayMonth()).setScale(5, BigDecimal.ROUND_HALF_UP);
            lastPartVolDt = curDt;
            map.put(curDt, partVol);
            diff = diff.subtract(partVol);
        }

        if (lastPartVolDt != null && !diff.equals(BigDecimal.ZERO)) {
            // округление на последний день месяца
            map.put(lastPartVolDt, map.get(lastPartVolDt).add(diff));
        }
        return map.get(dt);
    }

    /**
     * Получить объемы экономии по услугам лиц.счетов, рассчитанных по квартире
     *
     * @param calcStore - хранилище объемов
     * @param ko        - квартира
     * @param lstSelUsl - список ограничения услуг (например при распределении ОДН)
     */
    private void distODNeconomy(CalcStore calcStore, Ko ko, List<Usl> lstSelUsl) throws ErrorWhileChrg {
        // получить объемы экономии по всем лиц.счетам квартиры
        List<ChargePrep> lstChargePrep = ko.getKart().stream()
                .flatMap(t -> t.getChargePrep().stream())
                .filter(t -> t.getTp().equals(4) && lstSelUsl.size() == 0)
                .collect(Collectors.toList());

        // распределить экономию
        for (ChargePrep t : lstChargePrep) {

            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема в лиц.счете (когда были проживающие)
            List<UslVolKart> lstUslVolKart = calcStore.getChrgCountAmount().getLstUslVolKart().stream()
                    .filter(d -> d.kart.equals(t.getKart()) && !d.kpr.equals(BigDecimal.ZERO) && d.usl.equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета
            Utl.distBigDecimalByList(t.getVol(), lstUslVolKart, 5);

            // РАСПРЕДЕЛИТЬ по датам, детально, для услуги calcTp=47 (Тепл.энергия для нагрева ХВС (Кис.)) (когда были проживающие)
            List<UslPriceVolKart> lstUslPriceVolKart = calcStore.getChrgCountAmount().getLstUslPriceVolKart().stream()
                    .filter(d -> d.kart.equals(t.getKart()) && !d.kpr.equals(BigDecimal.ZERO) && d.usl.equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета, по датам
            Utl.distBigDecimalByList(t.getVol(), lstUslPriceVolKart, 5);

            // ПО СГРУППИРОВАННЫМ объемам до лиц.счетов, просто снять объем
            UslVolKartGrp uslValKartGrp = calcStore.getChrgCountAmount().getLstUslVolKartGrp().stream()
                    .filter(d -> d.kart.equals(t.getKart()) && d.usl.equals(t.getUsl()))
                    .findFirst().orElse(null);
            if (uslValKartGrp != null) {
                if (uslValKartGrp.vol.compareTo(BigDecimal.ZERO) > 0
                        && uslValKartGrp.vol.compareTo(t.getVol().abs()) > 0) {
                    uslValKartGrp.vol = uslValKartGrp.vol.add(t.getVol());
                } else {
                    throw new ErrorWhileChrg("ОШИБКА! Объем экономии ОДН =" + t.getVol()
                            + " больше чем собственный объем=" + uslValKartGrp.vol
                            + " по лиц. lsk="
                            + t.getKart().getLsk());
                }
            } else {
                throw new ErrorWhileChrg("ОШИБКА! Не найден объем для распределения экономии ОДН по лиц. lsk="
                        + t.getKart().getLsk());

            }

        }
    }
}