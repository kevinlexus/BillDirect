package com.dic.app.mm.impl;

import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.GenPenMng;
import com.dic.app.mm.GenPenProcessMng;
import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.*;
import com.dic.bill.dto.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
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
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
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


        log.info("Список уникальных усл.+орг.:");
        lstUslOrg.forEach(t-> log.info("usl={}, org={}", t.getUslId(), t.getOrgId()));

        // Расчет задолженности, подготовка для расчета пени
        debitThrMng.genDebitUsl(kart, calcStore, localStore);
    }

}