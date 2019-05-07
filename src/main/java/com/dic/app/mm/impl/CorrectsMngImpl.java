package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
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

    final private SaldoMng saldoMng;
    final private SaldoUslDAO saldoUslDAO;
    final private TuserDAO tuserDAO;
    final private ConfigApp configApp;

    @PersistenceContext
    private EntityManager em;

    public CorrectsMngImpl(SaldoMng saldoMng, SaldoUslDAO saldoUslDAO,
                           TuserDAO tuserDAO, ConfigApp configApp) {
        this.saldoMng = saldoMng;
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
        // сальдо, в тех лиц.сч., в которых есть еще и дебетовое
        List<SaldoUsl> lstSal = new ArrayList<>(
                saldoUslDAO.getSaldoUslWhereCreditAndDebitExists(period));
        // пользователь
        Tuser user = tuserDAO.getByCd("GEN");
        // первое число месяца
        Date dt = Utl.getDateFromPeriod(period);
        if (lstSal.size() > 0) {
            ChangeDoc changeDoc = ChangeDoc.ChangeDocBuilder.aChangeDoc()
                    .withDt(dt).withMg2(period).withMgchange(period)
                    .withUser(user).build();
            em.persist(changeDoc);

            // проводки
            ArrayList<SumUslOrgDTO> lstCorrects = new ArrayList<SumUslOrgDTO>();

            // уникальный список лиц.счетов
            List<Kart> lstKart = lstSal.stream().map(SaldoUsl::getKart).distinct().collect(Collectors.toList());
            for (Kart kart : lstKart) {
                // сальдо по данному лиц.сч.
                List<SumUslOrgDTO> lstSalKart = lstSal.stream().filter(t -> t.getKart().equals(kart))
                        .map(t -> new SumUslOrgDTO(t.getUsl().getId(), t.getOrg().getId(), t.getSumma()))
                        .collect(Collectors.toList());

                // организации с кредитовым сальдо, по которым есть так же дебетовое сальдо по другим услугам
                List<Integer> lstOrgId = lstSalKart.stream()
                        .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) < 0
                                && lstSalKart.stream().filter(d -> d.getSumma().compareTo(BigDecimal.ZERO) > 0)
                                .anyMatch(d -> d.getOrgId().equals(t.getOrgId()))
                        )
                        .map(SumUslOrgDTO::getOrgId)
                        .collect(Collectors.toList());

                log.info("сальдо до корректировок:");
                lstSalKart.forEach(t -> {
                    log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
                });

                for (Integer orgId : lstOrgId) {
                    log.info("организация orgId={}:", orgId);
                    // кред.сальдо по данной орг * -1
                    List<SumUslOrgDTO> lstCredOrg = lstSalKart.stream()
                            .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) < 0
                                    && t.getOrgId().equals(orgId)
                            )
                            .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma().negate()))
                            .collect(Collectors.toList());

                    // деб.сальдо по данной орг
                    List<SumUslOrgDTO> lstDebOrg = lstSalKart.stream()
                            .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                                    && t.getOrgId().equals(orgId))
                            .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma()))
                            .collect(Collectors.toList());
                    if (lstDebOrg.size() > 0) {
                        // распределить кредит по дебету, получить проводки
                        HashMap<Integer, Map<DistributableBigDecimal, BigDecimal>> mapCorr =
                                Utl.distListByListIntoMap(lstCredOrg, lstDebOrg, 2);

                        // поменять знак у корректировки снятия с кредитового сальдо
                        List<SumUslOrgDTO> lstCorrCred = mapCorr.get(0).entrySet().stream()
                                .map(k -> new SumUslOrgDTO(
                                        ((SumUslOrgDTO) k.getKey()).getUslId(),
                                        ((SumUslOrgDTO) k.getKey()).getOrgId(), k.getValue()))
                                .collect(Collectors.toList());
                        List<SumUslOrgDTO> lstCorrDeb = mapCorr.get(1).entrySet().stream()
                                .map(k -> new SumUslOrgDTO(
                                        ((SumUslOrgDTO) k.getKey()).getUslId(),
                                        ((SumUslOrgDTO) k.getKey()).getOrgId(), k.getValue().negate()))
                                .collect(Collectors.toList());

                        log.info("корректировки по кредиту:");
                        lstCorrCred.forEach(t -> {
                            log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
                            // проводки в T_CORRECTS_PAYMENTS, с другим знаком
                            saveCorrects(period, user, dt, changeDoc, kart, t.getUslId(), t.getOrgId(), t.getSumma().negate());
                        });
                        log.info("корректировки по дебету:");
                        lstCorrDeb.forEach(t -> {
                            log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
                            // проводки в T_CORRECTS_PAYMENTS, с другим знаком
                            saveCorrects(period, user, dt, changeDoc, kart, t.getUslId(), t.getOrgId(), t.getSumma().negate());
                        });

                        // сгруппировать с сальдо:
                        // корректировку по кредиту
                        saldoMng.groupByLstUslOrg(lstSalKart, lstCorrCred);
                        // корректировку по дебету
                        saldoMng.groupByLstUslOrg(lstSalKart, lstCorrDeb);
                    }
                }

                log.info("сальдо учётом корректировок:");
                lstSalKart.forEach(t -> {
                    log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
                });

                /*
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

                */
            }
        }
    }

    /**
     * Корректировки в T_CORRECTS_PAYMENTS
     *
     * @param period    - период
     * @param user      - пользователь
     * @param dt        - дата
     * @param changeDoc - документ по корректировке
     * @param kart      - лиц.счет
     * @param uslId     - Id услуги
     * @param orgId     - Id организации
     * @param summa     - сумма
     */
    @Override
    public void saveCorrects(String period, Tuser user, Date dt, ChangeDoc changeDoc, Kart kart,
                             String uslId, Integer orgId, BigDecimal summa) {
        CorrectPay corrPay = CorrectPay.CorrectPayBuilder.aCorrectPay()
                .withChangeDoc(changeDoc)
                .withDopl(period).withDt(dt)
                .withKart(kart)
                .withMg(period)
                .withUsl(em.find(Usl.class, uslId))
                .withOrg(em.find(Org.class, orgId))
                .withSumma(summa)
                .withUser(user)
                .build();
        em.persist(corrPay);
    }

}
