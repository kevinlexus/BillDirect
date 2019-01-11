package com.dic.app.mm.impl;

import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.StatesPrDAO;
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
    private SprParamMng sprParamMng;
    @PersistenceContext
    private EntityManager em;

    /**
     * Рассчитать начисление
     * Внимание! Расчет идёт по квартире (помещению), но информация группируется по лиц.счету(Kart)
     * так как теоретически может быть одинаковая услуга на разных лиц.счетах, но на одной квартире!
     *
     * @param calcStore - хранилище справочников
     * @param ko - Ko квартиры
     */
    @Override
    public void genChrg(CalcStore calcStore, Ko ko) throws WrongParam, ErrorWhileChrg {
        // получить основной лиц счет по связи klsk квартиры
        Kart kartMainByKlsk = kartMng.getKartMain(ko);
        // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
        int parVarCntKpr =
                Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"), 0D).intValue();
        // параметр учета проживающих для капремонта
        int parCapCalcKprTp =
                Utl.nvl(sprParamMng.getN1("CAP_CALC_KPR_TP"), 0D).intValue();

        ChrgCount chrgCount = new ChrgCount();

        // сохранить помещение
        chrgCount.setKo(ko);
        // получить все действующие счетчики квартиры и их объемы
        chrgCount.setLstMeterVol(meterDao.findMeterVolByKlsk(ko.getId(),
                calcStore.getCurDt1(), calcStore.getCurDt2()));
        chrgCount.getLstMeterVol().forEach(t -> {
            log.info("Check2: {}, {}, {}, {}, {}", t.getMeterId(), t.getUslId(), t.getVol(), t.getDtFrom(), t.getDtTo());
        });
        // получить объемы по счетчикам в пропорции на 1 день их работы
        Map<String, BigDecimal> mapDayMeterVol = meterMng.getPartDayMeterVol(chrgCount,
                calcStore);

        // цикл по дням месяца
        Calendar c = Calendar.getInstance();
        for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();
            //log.info("Date={}", curDt);
            // получить действующие, отсортированные услуги по квартире (по всем счетам)
            List<Nabor> lstNabor = naborMng.getValidNabor(ko, curDt);

            boolean isExistsMeterColdWater = false;
            boolean isExistsMeterHotWater = false;
            BigDecimal volMeterColdWater = BigDecimal.ZERO;
            BigDecimal volMeterHotWater = BigDecimal.ZERO;

            // объем по услуге, за рассчитанный день
            Map<Usl, UslPriceVol> mapUslPriceVol = new HashMap<>(30);

            for (Nabor nabor : lstNabor) {
                if (nabor.getUsl().isMain()) {
                    // по основным услугам
                    Integer fkCalcTp = nabor.getUsl().getFkCalcTp();
                    BigDecimal naborNorm = Utl.nvl(nabor.getNorm(), BigDecimal.ZERO);
                    BigDecimal naborVol = Utl.nvl(nabor.getVol(), BigDecimal.ZERO);
                    BigDecimal naborVolAdd = Utl.nvl(nabor.getVolAdd(), BigDecimal.ZERO);
                    // услуга с которой получить объем (иногда выполняется перенаправление, например для fkCalcTp=31)
                    Usl factUslVol = nabor.getUsl().getFactUslVol();
                    // ввод
                    Vvod vvod = nabor.getVvod();
                    // тип распределения ввода
                    Integer distTp = 0;
                    if (vvod != null) {
                        distTp = vvod.getDistTp();
                    }
                    Kart kartMain;
                    // получить основной лиц.счет, если указан явно
                    if (nabor.getKart().getParentKart() != null) {
                        kartMain = nabor.getKart();
                    } else {
                        kartMain = kartMainByKlsk;
                    }
                    // получить цены по услуге по лицевому счету из набора услуг!
                    DetailUslPrice detailUslPrice = naborMng.getDetailUslPrice(kartMain, nabor);

                    // получить кол-во проживающих по лицевому счету из набора услуг!
                    CountPers countPers = kartPrMng.getCountPersByDate(kartMain, nabor,
                            parVarCntKpr, parCapCalcKprTp, curDt);
                    SocStandart socStandart = null;
                    // получить наличие счетчика
                    boolean isMeterExist = false;
                    BigDecimal tempVol = BigDecimal.ZERO;
                    // объемы
                    BigDecimal dayVol = BigDecimal.ZERO;
                    BigDecimal dayVolOverSoc = BigDecimal.ZERO;
                    BigDecimal dayVolEmpty = BigDecimal.ZERO;

                    // площади
                    BigDecimal kartArea = Utl.nvl(kartMain.getOpl(), BigDecimal.ZERO);

                    BigDecimal area = BigDecimal.ZERO;
                    BigDecimal areaOverSoc = BigDecimal.ZERO;
                    BigDecimal areaEmpty = BigDecimal.ZERO;

                    if (Utl.in(fkCalcTp, 25)) {
                        // Текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                        area = kartArea;
                        dayVol = area.multiply(calcStore.getPartDayMonth());
                        socStandart = kartPrMng.getSocStdtVol(nabor, countPers);
                    } else if (Utl.in(fkCalcTp, 17, 18, 31)) {
                        // Х.В., Г.В., без уровня соцнормы/свыше, электроэнергия
                        // получить объем по нормативу в доле на 1 день
                        // узнать, работал ли хоть один счетчик в данном дне
                        isMeterExist = meterMng.isExistAnyMeter(chrgCount, nabor.getUsl().getId(), curDt);
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
                            tempVol = mapDayMeterVol.get(factUslVol.getId());
                            // в данном случае - объем уже в пропорции на 1 день
                            //log.info("uslId={}, dt={}, dayVol={}", nabor.getUsl().getId(), curDt, tempVol);
                        } else {
                            tempVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                            //log.info("uslId={}, dt={}, нет счетчика! объем по нормативу={}",
                            //        nabor.getUsl().getId(), curDt, tempVol);
                        }
                        if (countPers.isEmpty) {
                            // пустая квартира
                            dayVolEmpty = tempVol;
                            areaEmpty = kartArea;
                        } else {
                            // квартира с проживающими
                            dayVol = tempVol;
                            area = kartArea;
                        }
                        // для водоотведения
                        if (fkCalcTp.equals(17)) {
                            volMeterColdWater = tempVol;
                        } else if (fkCalcTp.equals(18)) {
                            volMeterHotWater = tempVol;
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
                        tempVol = volMeterColdWater.add(volMeterHotWater);
                        if (countPers.isEmpty) {
                            // пустая квартира
                            dayVolEmpty = tempVol;
                            areaEmpty = kartArea;
                        } else {
                            // квартира с проживающими
                            dayVol = tempVol;
                            area = kartArea;
                        }

                    } else if (Utl.in(fkCalcTp, 14)) {
                        // Отопление гкал. без уровня соцнормы/свыше
                        if (!Utl.nvl(kartMain.getPot(), BigDecimal.ZERO).equals(BigDecimal.ZERO)) {
                            // есть показания по Индивидуальному счетчику отопления (Бред!)
                            tempVol = Utl.nvl(kartMain.getMot(), BigDecimal.ZERO);
                        } else {
                            if (!kartArea.equals(BigDecimal.ZERO)) {
                                if (distTp.equals(1)) {
                                    // есть ОДПУ по отоплению гкал, начислить по распределению
                                    tempVol = naborVol;
                                } else if (Utl.in(distTp,4, 5)) {
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
                        if (countPers.isEmpty) {
                            // пустая квартира
                            dayVolEmpty = tempVol.multiply(calcStore.getPartDayMonth());
                            areaEmpty = kartArea;
                        } else {
                            // квартира с проживающими
                            dayVol = tempVol.multiply(calcStore.getPartDayMonth());
                            area = kartArea;
                        }
                    } else if (fkCalcTp.equals(7) && kartMain.getStatus().getId().equals(1)) {
                        // Найм (только по муниципальным квартирам) расчет на м2
                        area = kartArea;
                        dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                    } else if (Utl.in(fkCalcTp, 12)) {
                        // Антенна, код.замок
                        area = kartArea;
                        dayVol = calcStore.getPartDayMonth();
                    } else if (Utl.in(fkCalcTp, 20,21,23)) {
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
                        if (nabor.getUsl().getParentUsl()!=null) {
                            // получить объем из родительской услуги
                            UslPriceVol uslPriceVol = mapUslPriceVol.get(nabor.getUsl().getParentUsl());
                            if (uslPriceVol!=null && !uslPriceVol.isCounter) {
                                // только если нет счетчика в родительской услуге
                                area = kartArea;
                                // сложить все объемы родит.услуги, умножить на норматив текущей услуги
                                dayVol = (uslPriceVol.vol.add(uslPriceVol.volOverSoc).add(uslPriceVol.volEmpty))
                                        .multiply(naborNorm);
                            }

                        } else {
                            throw new ErrorWhileChrg("ОШИБКА! По услуге usl.id="+nabor.getUsl().getId()+
                                " отсутствует PARENT_USL");
                        }
                    } else if (fkCalcTp.equals(49)) {
                        // Вывоз мусора - кол-во прожив * цену (Кис.)
                        area = kartArea;
                        dayVol = BigDecimal.valueOf(countPers.kpr).multiply(calcStore.getPartDayMonth());
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
                    if (!dayVol.equals(BigDecimal.ZERO) || !dayVolOverSoc.equals(BigDecimal.ZERO)
                            || !dayVolEmpty.equals(BigDecimal.ZERO)) {
                        UslPriceVol uslPriceVol = UslPriceVol.UslPriceVolBuilder.anUslPriceVol()
                                .withKart(nabor.getKart()) // группировать по лиц.счету из nabor!
                                .withDtFrom(curDt).withDtTo(curDt).withDtFrom(curDt)
                                .withUsl(nabor.getUsl())
                                .withOrg(nabor.getOrg())
                                .withIsCounter(isMeterExist)
                                .withIsEmpty(countPers.isEmpty)
                                .withSocStdt(socStandart!=null?socStandart.norm:BigDecimal.ZERO)
                                .withPrice(detailUslPrice.price)
                                .withPriceOverSoc(detailUslPrice.priceOverSoc)
                                .withPriceEmpty(detailUslPrice.priceEmpt)
                                .withVol(dayVol)
                                .withVolOverSoc(dayVolOverSoc)
                                .withVolEmpty(dayVolEmpty)
                                .withArea(area)
                                .withAreaOverSoc(areaOverSoc)
                                .withAreaEmpty(areaEmpty)
                                .withKpr(countPers.kpr).withKprOt(countPers.kprOt).withKprWr(countPers.kprWr)
                                .withPartDayMonth(calcStore.getPartDayMonth())
                                .build();

/*
                        if (Utl.in(uslPriceVol.usl.getId(),"003")) {
                            log.info("РАСЧЕТ ДНЯ:");
                            log.info("dt:{}-{} usl={} org={} cnt={} " +
                                            "empt={} stdt={} " +
                                            "prc={} prcOv={} prcEm={} " +
                                            "vol={} volOv={} volEm={} ar={} arOv={} " +
                                            "arEm={} Kpr={} Ot={} Wrz={}",
                                    Utl.getStrFromDate(uslPriceVol.dtFrom, "dd"), Utl.getStrFromDate(uslPriceVol.dtTo, "dd"),
                                    uslPriceVol.usl.getId(), uslPriceVol.org.getId(), uslPriceVol.isCounter, uslPriceVol.isEmpty,
                                    uslPriceVol.socStdt, uslPriceVol.price, uslPriceVol.priceOverSoc, uslPriceVol.priceEmpty,
                                    uslPriceVol.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.volOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.volEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.area.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.areaOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.areaEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.kpr.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.kprOt.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    uslPriceVol.kprWr.setScale(4, BigDecimal.ROUND_HALF_UP));
                        }
*/
                        // сохранить рассчитанный объем по расчетному дню, для услуги Повыш коэфф.
                        mapUslPriceVol.put(nabor.getUsl(), uslPriceVol);
                        // сгруппировать по лиц.счету, услуге
                        chrgCount.groupUslPriceVol(uslPriceVol);
                    }
                }
            }

            // Блок умножения объем на цену (расчет в рублях)
        }

        // сохранить в объемы дома (для расчета ОДН и прочих распределений во вводах)
        calcStore.getChrgCountHouse().addChrgCount(chrgCount);

        // получить кол-во проживающих по лиц.счету
/*
        log.info("ИТОГО:");
        log.info("UslPriceVol:");
        BigDecimal amntVol = BigDecimal.ZERO;

        for (UslPriceVol t : chrgCount.getLstUslPriceVol()) {
            if (Utl.in(t.usl.getId(),"003")) {
                log.info("dt:{}-{} usl={} org={} cnt={} " +
                                "empt={} stdt={} " +
                                "prc={} prcOv={} prcEm={} " +
                                "vol={} volOv={} volEm={} ar={} arOv={} " +
                                "arEm={} Kpr={} Ot={} Wrz={}",
                        Utl.getStrFromDate(t.dtFrom, "dd"), Utl.getStrFromDate(t.dtTo, "dd"),
                        t.usl.getId(), t.org.getId(), t.isCounter, t.isEmpty,
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
}