package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.model.scott.*;
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис корректировок
 *
 * @version 1.00
 */
@Slf4j
@Service
public class CorrectsMngImpl implements CorrectsMng {

    final private SaldoUslDAO saldoUslDAO;

    final private TuserDAO tuserDAO;
    final private ConfigApp configApp;

    @PersistenceContext
    private EntityManager em;

    public CorrectsMngImpl(SaldoUslDAO saldoUslDAO, TuserDAO tuserDAO, ConfigApp configApp) {
        this.saldoUslDAO = saldoUslDAO;
        this.tuserDAO = tuserDAO;
        this.configApp = configApp;
    }

    /**
     * Корректировка взаимозачета сальдо, исключая некоторые услуги
     */
    @Override
    public void corrPayByCreditSalExceptSomeUsl() throws WrongParam {
        // текущий период
        String period = configApp.getPeriod();
        // кредитовое сальдо, в тех лиц.сч., в которых есть еще и дебетовое
        List<SaldoUsl> lstCred = new ArrayList<>(
                saldoUslDAO.getCreditSaldoUslWhereDebitExists(period));
        // уникальный список лиц.счетов
        List<Kart> lstKart = lstCred.stream().map(SaldoUsl::getKart).distinct().collect(Collectors.toList());
        for (Kart kart : lstKart) {
            // кредитовое сальдо * -1 по данному лиц.сч.
            List<SumUslOrgDTO> lstCredKart = lstCred.stream().filter(t -> t.getKart().equals(kart))
                    .map(t -> new SumUslOrgDTO(t.getUsl().getId(), t.getOrg().getId(), t.getSumma().negate()))
                    .collect(Collectors.toList());
            // деб.сальдо по данному лс
            List<SumUslOrgDTO> lstDebKart = saldoUslDAO
                    .getSaldoUslByLsk(kart.getLsk(), period).stream()
                    .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma()))
                    .collect(Collectors.toList());
            HashMap<Integer, Map<DistributableBigDecimal, BigDecimal>> mapCorr =
                    Utl.distListByListIntoMap(lstCredKart, lstDebKart, 2);

            // пользователь
            Tuser user = tuserDAO.getByCd("GEN");

            // первое число месяца
            Date dt = Utl.getDateFromPeriod(period);
            ChangeDoc changeDoc = ChangeDoc.ChangeDocBuilder.aChangeDoc()
                .withDt(dt).withMg2(period).withMgchange(period)
                    .withUser(user).build();
            em.persist(changeDoc);
            // корректировки снятия с кредит сальдо.
            mapCorr.get(0).forEach((key, value) -> {
                SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) key;
                CorrectPay corrPay = CorrectPay.CorrectPayBuilder.aCorrectPay()
                        .withChangeDoc(changeDoc)
                        .withDopl(period).withDt(dt).withKart(kart).withMg(period)
                        .withUsl(em.find(Usl.class, sumUslOrgDTO.getUslId()))
                        .withOrg(em.find(Org.class, sumUslOrgDTO.getOrgId()))
                        .withSumma(value.negate())
                        .withUser(user)
                        .build();
                em.persist(corrPay);
            });

            // корректировки постановки на деб.сальдо
            mapCorr.get(1).forEach((key, value) -> {
                SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) key;
                CorrectPay corrPay = CorrectPay.CorrectPayBuilder.aCorrectPay()
                        .withChangeDoc(changeDoc)
                        .withDopl(period).withDt(dt).withKart(kart).withMg(period)
                        .withUsl(em.find(Usl.class, sumUslOrgDTO.getUslId()))
                        .withOrg(em.find(Org.class, sumUslOrgDTO.getOrgId()))
                        .withSumma(value)
                        .withUser(user)
                        .build();
                em.persist(corrPay);
            });

        }
    }

}
