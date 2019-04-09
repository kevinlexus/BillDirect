package com.dic.app.mm.impl;

import com.dic.app.Config;
import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.KwtpMg;
import com.dic.bill.model.scott.Param;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

/**
 * Сервис распределения оплаты
 */
public class DistPayMngImpl implements DistPayMng {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private SaldoMng saldoMng;
    @Autowired
    private ConfigApp configApp;

    /**
     * Распределить платеж (запись в C_KWTP_MG)
     */
    public void distKwtpMg(KwtpMg kwtpMg) {
        Kart kart = kwtpMg.getKart();
        String currPeriod = configApp.getPeriod();
        // сперва получить входящее сальдо
        List<SumUslOrgDTO> outSal = saldoMng.getOutSal(kart, currPeriod,
                true, false, false, false, false);

    }
}
