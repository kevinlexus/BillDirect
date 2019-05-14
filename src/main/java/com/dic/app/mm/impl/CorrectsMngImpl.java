package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.CorrectsMng;
import com.dic.bill.dao.ChangeDocDAO;
import com.dic.bill.dao.CorrectPayDAO;
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
    final private CorrectPayDAO correctPayDAO;
    final private ChangeDocDAO changeDocDAO;
    @PersistenceContext
    private EntityManager em;

    public CorrectsMngImpl(SaldoMng saldoMng, SaldoUslDAO saldoUslDAO,
                           TuserDAO tuserDAO, ConfigApp configApp,
                           CorrectPayDAO correctPayDAO,
                           ChangeDocDAO changeDocDAO) {
        this.saldoMng = saldoMng;
        this.saldoUslDAO = saldoUslDAO;
        this.tuserDAO = tuserDAO;
        this.configApp = configApp;
        this.correctPayDAO = correctPayDAO;
        this.changeDocDAO = changeDocDAO;
    }

    /**
     * Корректировка взаимозачета сальдо, исключая некоторые услуги
     *
     * @param var - вариант проводки (0- распр.кредит по дебету по выбранным орг. Кис. выполняется после 15 числа
     *            1 - распр.кредит по 003 орг - выполняется 31 числа или позже, до перехода)
     * @param dt  - дата корректировки
     */
    @Override
    public void corrPayByCreditSal(int var, Date dt) throws WrongParam {
        log.info("Корректировка сальдо. ver=1.00,  вариант={}, дата={}", var, dt);

        // текущий период
        String period = configApp.getPeriod();
        // пользователь
        Tuser user = tuserDAO.getByCd("GEN");
        String cdTp = null;
        if (var == 0) {
            cdTp = "JavaCorrPayByCreditSal-1";
        } else if (var == 1) {
            cdTp = "JavaCorrPayByCreditSal-2";
        }
        // удалить предыдущую корректировку
        changeDocDAO.deleteChangeDocByCdTp(cdTp);
        correctPayDAO.deleteCorrectPayByChangeDoc(cdTp);

        if (var == 0) {
            // распр.кредит по дебету по выбранным орг. Кис. выполняется после 15 числа
            // сальдо, в тех лиц.сч., в которых есть еще и дебетовое
            List<SaldoUsl> lstSal = new ArrayList<>(
                    saldoUslDAO.getSaldoUslWhereCreditAndDebitExists(period));
            if (lstSal.size() > 0) {
                // создать документ по корректировке
                ChangeDoc changeDoc = ChangeDoc.ChangeDocBuilder.aChangeDoc()
                        .withDt(dt).withMg2(period).withMgchange(period)
                        .withCdTp(cdTp)
                        .withText("Корректировка кредитового сальдо при наличии дебетового")
                        .withUser(user).build();
                em.persist(changeDoc);

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
                    lstSalKart.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
                    log.info("итого:{}", lstSalKart.stream().map(SumUslOrgDTO::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add));

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
                            distCredByDeb(period, user, dt, changeDoc, kart, lstSalKart, lstCredOrg, lstDebOrg);
                        }
                    }

                    log.info("сальдо с учётом 1-ой корректировки:");
                    lstSalKart.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
                    log.info("итого:{}", lstSalKart.stream().map(SumUslOrgDTO::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add));

                    // кред.сальдо * -1
                    List<SumUslOrgDTO> lstCred = lstSalKart.stream()
                            .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) < 0)
                            .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma().negate()))
                            .collect(Collectors.toList());
                    if (lstCred.size() > 0) {
                        // получить дебетовое сальдо
                        List<SumUslOrgDTO> lstDeb = lstSalKart.stream()
                                .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                                .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma()))
                                .collect(Collectors.toList());
                        if (lstDeb.size() > 0) {
                            distCredByDeb(period, user, dt, changeDoc, kart, lstSalKart, lstCred, lstDeb);
                        }
                    }

                    log.info("сальдо с учётом 2-ой корректировки:");
                    lstSalKart.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
                    log.info("итого:{}", lstSalKart.stream().map(SumUslOrgDTO::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add));
                }
            }
        } else if (var == 1) {
            // распр.кредит по 003 орг - выполняется 31 числа или позже, до перехода)
            // сальдо, в тех лиц.сч., в которых по кредитовому сальдо по услуге 003 есть еще и дебетовое по другим услугам
            List<SaldoUsl> lstSal = new ArrayList<>(
                    saldoUslDAO.getSaldoUslWhereCreditAndDebitExistsWoPayByUsl("003", period));
            // уникальный список лиц.счетов
            List<Kart> lstKart = lstSal.stream().map(SaldoUsl::getKart).distinct().collect(Collectors.toList());
            for (Kart kart : lstKart) {
                log.info("сальдо до корректировок:");
                // сальдо по данному лиц.сч.
                List<SumUslOrgDTO> lstSalKart = lstSal.stream().filter(t -> t.getKart().equals(kart))
                        .map(t -> new SumUslOrgDTO(t.getUsl().getId(), t.getOrg().getId(), t.getSumma()))
                        .collect(Collectors.toList());
                // создать документ по корректировке
                ChangeDoc changeDoc = ChangeDoc.ChangeDocBuilder.aChangeDoc()
                        .withDt(dt).withMg2(period).withMgchange(period)
                        .withCdTp(cdTp)
                        .withText("Корректировка кредитового сальдо по 003 услуге, при наличии дебетового по другим " +
                                "услугам и при отсутствии оплаты")
                        .withUser(user).build();
                em.persist(changeDoc);

                lstSalKart.forEach(t -> log.info("usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
                log.info("итого:{}", lstSalKart.stream()
                        .map(SumUslOrgDTO::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add));

                // кред.сальдо * -1 по услуге 003
                List<SumUslOrgDTO> lstCred = lstSalKart.stream()
                        .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) < 0 && t.getUslId().equals("003"))
                        .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma().negate()))
                        .collect(Collectors.toList());

                // получить дебетовое сальдо
                List<SumUslOrgDTO> lstDeb = lstSalKart.stream()
                        .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                        .map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma()))
                        .collect(Collectors.toList());
                if (lstDeb.size() > 0) {
                    distCredByDeb(period, user, dt, changeDoc, kart, lstSalKart, lstCred, lstDeb);
                }
                log.info("сальдо с учётом корректировки:");
                lstSalKart.forEach(t -> log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma()));
                log.info("итого:{}", lstSalKart.stream().map(SumUslOrgDTO::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add));
            }
        }
    }

    /**
     * Распределить кредит по дебету, создать проводки
     *
     * @param period     - текущий период
     * @param user       - пользователь, которым провести корректировки
     * @param dt         - дата корректировок
     * @param changeDoc  - документ
     * @param kart       - лиц.счет
     * @param lstSalKart - сальдо по лиц.счету
     * @param lstCred    - кредитовое сальдо
     * @param lstDeb     - дебетовое сальдо
     */
    private void distCredByDeb(String period, Tuser user, Date dt, ChangeDoc changeDoc,
                               Kart kart, List<SumUslOrgDTO> lstSalKart,
                               List<SumUslOrgDTO> lstCred, List<SumUslOrgDTO> lstDeb) throws WrongParam {
        // распределить кредит по дебету, получить проводки
        HashMap<Integer, Map<DistributableBigDecimal, BigDecimal>> mapCorr =
                Utl.distListByListIntoMap(lstCred, lstDeb, 2);

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
