package com.dic.app.mm.impl;

import com.dic.app.mm.DebitByLskThrMng;
import com.dic.app.mm.GenPenProcessMng;
import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.*;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Сервис формирования задолженностей и пени
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
public class GenPenProcessMngImpl implements GenPenProcessMng {

    private final PenDAO penDao;
    private final ChargeDAO chargeDao;
    private final ChangeDAO changeDao;
    private final KwtpMgDAO kwtpMgDao;
    private final CorrectPayDAO correctPayDao;
    private final ChargePayDAO chargePayDAO;
    private final ReferenceMng refMng;
    private final DebitByLskThrMng debitByLskThrMng;

    @PersistenceContext
    private EntityManager em;

    public GenPenProcessMngImpl(PenDAO penDao, ChargeDAO chargeDao,
                                ChangeDAO changeDao, KwtpMgDAO kwtpMgDao,
                                CorrectPayDAO correctPayDao,
                                ChargePayDAO chargePayDAO, ReferenceMng refMng, DebitByLskThrMng debitByLskThrMng) {
        this.penDao = penDao;
        this.chargeDao = chargeDao;
        this.changeDao = changeDao;
        this.kwtpMgDao = kwtpMgDao;
        this.correctPayDao = correctPayDao;
        this.chargePayDAO = chargePayDAO;
        this.refMng = refMng;
        this.debitByLskThrMng = debitByLskThrMng;
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
            genDebitPen(calcStore, kart);
        }
    }

    /**
     * Рассчет задолженности и пени по лиц.счету
     *
     * @param calcStore - хранилище справочников
     * @param kart      - лиц.счет
     */
    private void genDebitPen(CalcStore calcStore, Kart kart) throws ErrorWhileChrgPen {
        // метод в разработке с 09.12.20
        Integer period = calcStore.getPeriod();
        Integer periodBack = calcStore.getPeriodBack();

        // сформировать движение по лиц.счету (для пени - не нужно, сделал сюда, чтобы выполнялось многопоточно)
        chargePayDAO.genChrgPay(kart.getLsk(), 0, calcStore.getGenDt());

        // загрузить все финансовые операции по лиц.счету
        CalcStoreLocal localStore = new CalcStoreLocal();
        // задолженность предыдущего периода
        localStore.setLstDebFlow(chargePayDAO.getDebitByLsk(kart.getLsk(), periodBack));
        // текущее начисление - 2
        localStore.setChrgSum(chargeDao.getChargeByPeriodAndLsk(kart.getLsk()));
        // перерасчеты - 5
        localStore.setLstChngFlow(changeDao.getChangeByPeriodAndLsk(kart.getLsk()));
        // оплата - 3
        localStore.setLstPayFlow(kwtpMgDao.getKwtpMgByPeriodAndLsk(kart.getLsk()));
        // корректировки оплаты - 6
        localStore.setLstPayCorrFlow(correctPayDao.getCorrectByPeriodAndLsk(kart.getLsk(), String.valueOf(period)));

        // преобразовать String код reu в int, для ускорения фильтров
        localStore.setReuId(Integer.parseInt(kart.getUk().getReu()));

        // версия расчета в целом по лиц.счету
        debitByLskThrMng.genDeb(kart, calcStore, localStore);
    }

}