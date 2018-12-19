package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.NaborMng;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Nabor;
import com.dic.bill.model.scott.StatePr;
import com.dic.bill.model.scott.Usl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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
    @PersistenceContext
    private EntityManager em;

    /**
     * Рассчитать начисление
     *
     * @param calcStore - хранилище справочников
     * @param kart      - лиц.счет
     */
    @Override
    public void genChrg(CalcStore calcStore, Kart kart) {
        List<StatePr> lstStatesPr = statesPrDao.findByDate(kart.getLsk(),
                calcStore.getCurDt1(), calcStore.getCurDt2());

        // цикл по дням месяца
        Calendar c = Calendar.getInstance();
        for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
            Date curDt = c.getTime();
            log.info("Date={}", curDt);
            List<Nabor> lst = naborMng.getValidNabor(kart, curDt);
            lst.forEach(t-> {
                log.info("Usl.id={}, name={}", t.getUsl().getId(), t.getUsl().getName());
            });
        }
        // получить кол-во проживающих по лиц.счету
                    log.info("Расчет кол-ва проживающих");
        //kartMng.getPersCountByDate(kart);
        //log.info("Расчет начисления");
    }


    @Override
    public void countPers() {

    }

}