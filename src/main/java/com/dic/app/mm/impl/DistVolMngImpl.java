package com.dic.app.mm.impl;

import com.dic.app.mm.DistVolMng;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.UslVolKart;
import com.dic.bill.dto.UslVolKartGrp;
import com.dic.bill.dto.UslVolVvod;
import com.dic.bill.mm.ObjParMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;

/**
 * Сервис распределения объемов по дому
 * ОДН, и прочие объемы
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
public class DistVolMngImpl implements DistVolMng {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private ProcessMng processMng;
    @Autowired
    private KartDAO kartDAO;
    @Autowired
    private GenChrgProcessMng genChrgProcessMng;
    @Autowired
    private ObjParMng objParMng;

    // норматив по эл.энерг.
    final BigDecimal ODN_EL_NORM = new BigDecimal(2.7D);

    /**
     * Распределить объемы по вводу (по всем вводам, если reqConf.vvod == null)
     *
     * @param reqConf - параметры запроса
     */
    @Override
    public void distVolByVvod(RequestConfig reqConf) throws ErrorWhileChrgPen, WrongParam, WrongGetMethod {

        // загрузить справочники
        CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);
        Vvod vvod = reqConf.getVvod();
        // объем для распределения
        BigDecimal kub = Utl.nvl(vvod.getKub(), BigDecimal.ZERO);
        Usl usl = vvod.getUsl();

        int tp = -1;
        if (Utl.in(usl.getFkCalcTp(), 3, 17, 4, 18, 31, 38, 40)) {
            if (Utl.in(usl.getFkCalcTp(), 3, 17, 38)) {
                // х.в.
                tp = 0;
            } else if (Utl.in(usl.getFkCalcTp(), 4, 18, 40)) {
                // г.в.
                tp = 1;
            } else if (Utl.in(usl.getFkCalcTp(), 31)) {
                // эл.эн.
                tp = 2;
            }
        }

        // сбор информации, для расчета ОДН, подсчета итогов
        // кол-во лиц.счетов, объемы, кол-во прожив.
        // собрать информацию об объемах по лиц.счетам принадлежащим вводу
        processMng.genProcessAll(reqConf, calcStore);

        // объемы по дому:
        log.info("Объемы по лиц.счетам, вводу:");
        for (UslVolKart t : calcStore.getChrgCountAmount().getLstUslVolKart()) {
            if (Utl.in(t.usl.getId(), "053")) {
                log.info("lsk={} usl={} cnt={} " +
                                "empt={} resid={} " +
                                "vol={} area={} kpr={}",
                        t.kart.getLsk(),
                        t.usl.getId(), t.isCounter, t.isEmpty, t.isResidental,
                        t.vol.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.area.setScale(5, BigDecimal.ROUND_HALF_UP),
                        t.kpr.setScale(5, BigDecimal.ROUND_HALF_UP));
            }
        }

        // объемы по вводу
        if (usl.getFkCalcTp().equals(14)) {
            // Отопление Гкал
            for (UslVolVvod t : calcStore.getChrgCountAmount().getLstUslVolVvod()) {
                //log.info("usl={}, cnt={}, empt={}, resid={}, t.vol={}, t.area={}",
                //        t.usl.getId(), t.isCounter, t.isEmpty, t.isResidental, t.vol, t.area);
                if (!t.isResidental) {
                    // сохранить объемы по вводу для статистики
                    // площадь по нежилым помещениям
                    vvod.setOplAr(Utl.nvl(vvod.getOplAr(), BigDecimal.ZERO).add(t.area));
                }
                // площадь по вводу
                vvod.setOplAdd(Utl.nvl(vvod.getOplAdd(), BigDecimal.ZERO).add(t.area));
            }
            // округлить объемы по вводу
            vvod.setOplAr(vvod.getOplAr().setScale(5, RoundingMode.HALF_UP));
            vvod.setOplAdd(vvod.getOplAdd().setScale(5, RoundingMode.HALF_UP));
        }

        // ИТОГО:
        // площадь
        BigDecimal areaVvod = calcStore.getChrgCountAmount().getLstUslVolVvod()
                .stream().map(t -> t.area).reduce(BigDecimal.ZERO, BigDecimal::add);
        // объем
        BigDecimal volAmnt = calcStore.getChrgCountAmount().getLstUslVolVvod()
                .stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
        // кол-во прожив
        BigDecimal kprAmnt = calcStore.getChrgCountAmount().getLstUslVolVvod()
                .stream().map(t -> t.kpr).reduce(BigDecimal.ZERO, BigDecimal::add);

        // получить лимиты распределения по законодательству
        LimitODN limitODN = calcLimit(vvod.getHouse().getKo(), tp, kprAmnt, areaVvod);

        // ОЧИСТКА информации ОДН
        clearODN(vvod);

        if (kub != null) {
            if (usl.getFkCalcTp().equals(14)) {
                // Отопление Гкал, распределить по площади
                if (!areaVvod.equals(BigDecimal.ZERO)) {
                    for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
                        BigDecimal volForDist = kub.setScale(5, RoundingMode.HALF_UP);
                        BigDecimal vol = volForDist.multiply(t.area.divide(areaVvod, 5, RoundingMode.HALF_UP))
                                .setScale(5, RoundingMode.HALF_UP);
                        for (Nabor nabor : t.kart.getNabor()) {
                            if (nabor.getUsl().equals(usl)) {
                                nabor.setVol(vol);
                                log.info("Распределено: lsk={}, usl={}, kub={}, vol={}, area={}, areaVvod={}",
                                        t.kart.getLsk(), usl.getId(), kub, vol, t.area, areaVvod);
                            }
                        }
                    }
                }
            }
        }

        log.info("Итоговые объемы по вводу:");
        log.info("oplAdd={}, oplAr={}", vvod.getOplAdd(), vvod.getOplAr());

        // РАСПРЕДЕЛИТЬ объемы в домах с ОДПУ


        // РАСПРЕДЕЛИТЬ объемы в домах без ОДПУ

    }

    private void clearODN(Vvod vvod) {
        // почистить нормативы (ограничения)
        vvod.setNrm(null);

        for (Nabor nabor : vvod.getNabor()) {
            // удалить информацию по распр.ОДН.по информационным записям
            Iterator<Charge> itr = nabor.getKart().getCharge().iterator();
            while (itr.hasNext()) {
                Charge charge = itr.next();
                if (charge.getUsl().equals(vvod.getUsl()) && charge.getType().equals(5)) {
                    em.remove(charge);
                }
            }
            // удалить информацию по корректировкам ОДН
            Iterator<ChargePrep> itr2 = nabor.getKart().getChargePrep().iterator();
            while (itr2.hasNext()) {
                ChargePrep chargePrep = itr2.next();
                if (chargePrep.getUsl().equals(vvod.getUsl()) && chargePrep.getType().equals(4)) {
                    em.remove(chargePrep);
                }
            }
            
        }
    }

    /**
     * Рассчитать лимиты распределения по законодательству
     *  @param houseKo - Ko дома
     * @param tp      - тип услуги (0 - х.в., 1- г.в., 2 - эл.эн.)
     * @param cntKpr  - кол во прожив. по вводу
     * @param area    - площадь по вводу
     */
    private LimitODN calcLimit(Ko houseKo, int tp, BigDecimal cntKpr, BigDecimal area) throws WrongParam, WrongGetMethod {
        LimitODN limitODN = new LimitODN();

        if (Utl.in(tp, 0, 1)) {
            // х.в. г.в.
            //расчитать лимит распределения
            //если кол-во прожив. > 0
            if (!cntKpr.equals(BigDecimal.ZERO)) {
                // площадь на одного проживающего
                final BigDecimal oplMan = area.divide(cntKpr, 5, BigDecimal.ROUND_HALF_UP);
                final BigDecimal oplLiter = oplLiter(oplMan.intValue());
                limitODN.limitVol = oplLiter
                        .divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP);
                limitODN.odnNorm = limitODN.limitVol;
                final BigDecimal limitArea = oplLiter
                        .divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP);

            }

        } else if (tp == 2) {
            // эл.эн.
            // площадь общ.имущ., норматив, объем на площадь
            BigDecimal areaProp = objParMng.getBd(houseKo, "area_general_property");
            limitODN.odnNorm = ODN_EL_NORM;
        }

        return limitODN;
    }

    /**
     * Распределить объемы в домах с ОДПУ
     *
     * @param reqConf   - параметры запроса
     * @param calcStore - хранилище справочников
     */
    private void distVolWithODPU(RequestConfig reqConf, CalcStore calcStore) {


    }

    /**
     * Распределить объемы в домах без ОДПУ
     *
     * @param vvod
     * @param isMultiThreads
     */
    private void distVolWithoutODPU(Vvod vvod, boolean isMultiThreads) {

    }


    /**
     * таблица для возврата норматива потребления (в литрах) по соотв.площади на человека
     *
     * @param oplMan - площадь на человека
     * @return
     */
    public BigDecimal oplLiter(int oplMan) {
        double val;
        switch (oplMan) {
            case 1:
                val = 2;
                break;
            case 2:
                val = 2;
                break;
            case 3:
                val = 2;
                break;
            case 4:
                val = 10;
                break;
            case 5:
                val = 10;
                break;
            case 6:
                val = 10;
                break;
            case 7:
                val = 10;
                break;
            case 8:
                val = 10;
                break;
            case 9:
                val = 10;
                break;
            case 10:
                val = 9;
                break;
            case 11:
                val = 8.2;
                break;
            case 12:
                val = 7.5;
                break;
            case 13:
                val = 6.9;
                break;
            case 14:
                val = 6.4;
                break;
            case 15:
                val = 6.0;
                break;
            case 16:
                val = 5.6;
                break;
            case 17:
                val = 5.3;
                break;
            case 18:
                val = 5.0;
                break;
            case 19:
                val = 4.7;
                break;
            case 20:
                val = 4.5;
                break;
            case 21:
                val = 4.3;
                break;
            case 22:
                val = 4.1;
                break;
            case 23:
                val = 3.9;
                break;
            case 24:
                val = 3.8;
                break;
            case 25:
                val = 3.6;
                break;
            case 26:
                val = 3.5;
                break;
            case 27:
                val = 3.3;
                break;
            case 28:
                val = 3.2;
                break;
            case 29:
                val = 3.1;
                break;
            case 30:
                val = 3.0;
                break;
            case 31:
                val = 2.9;
                break;
            case 32:
                val = 2.8;
                break;
            case 33:
                val = 2.7;
                break;
            case 34:
                val = 2.6;
                break;
            case 35:
                val = 2.6;
                break;
            case 36:
                val = 2.5;
                break;
            case 37:
                val = 2.4;
                break;
            case 38:
                val = 2.4;
                break;
            case 39:
                val = 2.3;
                break;
            case 40:
                val = 2.3;
                break;
            case 41:
                val = 2.2;
                break;
            case 42:
                val = 2.1;
                break;
            case 43:
                val = 2.1;
                break;
            case 44:
                val = 2;
                break;
            case 45:
                val = 2;
                break;
            case 46:
                val = 2;
                break;
            case 47:
                val = 1.9;
                break;
            case 48:
                val = 1.9;
                break;
            case 49:
                val = 1.8;
                break;
            default:
                val = 1.8;

        }

        return BigDecimal.valueOf(val);
    }

    /**
     * DTO для хранения лимитов ОДН
     */
    class LimitODN {
        BigDecimal odnNorm = BigDecimal.ZERO;
        // допустимый лимит ОДН по законодательству (общий)
        BigDecimal limitVol = BigDecimal.ZERO;
        // допустимый лимит ОДН на 1 м2
        BigDecimal limitArea = BigDecimal.ZERO;
    }

}