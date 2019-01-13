package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.UslVolKart;
import com.dic.bill.dto.UslVolVvod;
import com.dic.bill.model.scott.Usl;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.stream.Stream;

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
    public void distVolByVvod(RequestConfig reqConf) throws ErrorWhileChrgPen {

        // загрузить справочники
        CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);

        // сбор информации, для расчета ОДН, подсчета итогов
        // кол-во лиц.счетов, объемы, кол-во прожив.
        // собрать информацию об объемах по лиц.счетам принадлежащим вводу
        processMng.genProcessAll(reqConf, calcStore);

        // объемы по дому:
        log.info("Объемы по лиц.счетам, вводу:");
        for (UslVolKart t : calcStore.getChrgCountAmount().getLstUslVolKart()) {
            if (Utl.in(t.usl.getId(),"003")) {
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

        log.info("Объемы по вводу:");
        for (UslVolVvod t : calcStore.getChrgCountAmount().getLstUslVolVvod()) {
            if (Utl.in(t.usl.getId(),"003")) {
                log.info("usl={}, cnt={}, empt={}, resid={}, t.vol={}, t.area={}",
                        t.usl.getId(), t.isCounter, t.isEmpty, t.isResidental, t.vol, t.area);
            }
        }

        // РАСПРЕДЕЛИТЬ объемы в домах с ОДПУ





        // РАСПРЕДЕЛИТЬ объемы в домах без ОДПУ

    }

    /**
     * Распределить объемы в домах с ОДПУ
     * @param reqConf - параметры запроса
     * @param calcStore - хранилище справочников
     */
    private void distVolWithODPU(RequestConfig reqConf, CalcStore calcStore) {




    }

    /**
     * Распределить объемы в домах без ОДПУ
     * @param vvod
     * @param isMultiThreads
     */
    private void distVolWithoutODPU(Vvod vvod, boolean isMultiThreads) {

    }

}