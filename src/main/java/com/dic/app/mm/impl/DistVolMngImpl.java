package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.ChrgCount;
import com.dic.bill.dto.ChrgCountHouse;
import com.dic.bill.dto.UslPriceVol;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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

    /**
     * Распределить объемы по вводу (по всем вводам, если reqConf.vvod == null)
     * @param reqConf - параметры запроса
     */
    @Override
    public void distVolByVvod(RequestConfig reqConf) {

        // загрузить справочники
        CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);

        // сбор информации, для расчета ОДН, подсчета итогов
        // кол-во лиц.счетов, объемы, кол-во прожив.

        // РАСПРЕДЕЛИТЬ объемы в домах с ОДПУ





        // РАСПРЕДЕЛИТЬ объемы в домах без ОДПУ

    }

    /**
     * Распределить объемы в домах с ОДПУ
     * @param reqConf - параметры запроса
     * @param calcStore - хранилище справочников
     */
    private void distVolWithODPU(RequestConfig reqConf, CalcStore calcStore)
            throws ErrorWhileChrgPen, WrongParam, ErrorWhileChrg {


        // собрать информацию об объемах по лиц.счетам принадлежащим вводу
        if (reqConf.isMultiThreads()) {
            // в потоках
            processMng.genProcessAll(reqConf, calcStore);
        } else {
            // тестирование, однопоточно, непосредственный вызов
            // получить distinct klsk помещений, выполнить расчет
            List<Integer> lstItem = kartDAO.getKoByVvod(reqConf.getVvod()).stream().map(t -> t.getId()).collect(Collectors.toList());
            for (Integer klskId : lstItem) {
                genChrgProcessMng.genChrg(calcStore, em.find(Ko.class, klskId));
            }

        }

        // объемы по дому:
        log.info("Объемы по дому:");
        for (ChrgCount d : calcStore.getChrgCountHouse().getLstChrgCount()) {
            for (UslPriceVol t : d.getLstUslPriceVol()) {
                if (Utl.in(t.usl.getId(),"003")) {
                    log.info("lsk={} usl={} cnt={} " +
                                    "empt={} " +
                                    "vol={} volOvSc={} volEmpt={} area={} areaOvSc={} " +
                                    "areaEmpt={} kpr={} kprOt={} kprWr={}",
                            t.kart.getLsk(),
                            t.usl.getId(), t.isCounter, t.isEmpty,
                            t.vol.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.volOverSoc.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.volEmpty.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.area.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.areaOverSoc.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.areaEmpty.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.kpr.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.kprOt.setScale(5, BigDecimal.ROUND_HALF_UP),
                            t.kprWr.setScale(5, BigDecimal.ROUND_HALF_UP));
                }
            }
        }


    }

    /**
     * Распределить объемы в домах без ОДПУ
     * @param vvod
     * @param isMultiThreads
     */
    private void distVolWithoutODPU(Vvod vvod, boolean isMultiThreads) {

    }

}