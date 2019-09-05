package com.dic.app.mm.impl;

import com.dic.app.mm.RegistryMng;
import com.dic.bill.dao.*;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.stream.Collectors.toMap;

/**
 * Сервис работы с различными реестрами
 *
 * @author lev
 * @version 1.00
 */
@Slf4j
@Service
public class RegistryMngImpl implements RegistryMng {

    @PersistenceContext
    private EntityManager em;
    private final PenyaDAO penyaDAO;
    private final MeterDAO meterDAO;
    private final EolinkMng eolinkMng;
    private final KartMng kartMng;


    public RegistryMngImpl(EntityManager em,
                           PenyaDAO penyaDAO, MeterDAO meterDAO, EolinkMng eolinkMng, KartMng kartMng) {
        this.em = em;
        this.penyaDAO = penyaDAO;
        this.meterDAO = meterDAO;
        this.eolinkMng = eolinkMng;
        this.kartMng = kartMng;
    }

    /**
     * Сформировать реест задолженности по лиц.счетам для Сбербанка
     */
    @Override // метод readOnly - иначе вызывается масса hibernate.AutoFlush - тормозит в Полыс, ред.04.09.2019
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void genDebitForSberbank() {
        log.info("Начало формирования реестра задолженности по лиц.счетам для Сбербанка");
        Path path = Paths.get("c:\\temp\\reestr1.txt");
        // дата формирования
        Date dt = new Date();
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            DebitRegistry debitRegistry = new DebitRegistry();
            for (Kart kart : penyaDAO.getKartWhereDebitExists()) {
                Optional<Eolink> eolink = kart.getEolink().stream().findFirst();
                // взять первый актуальный объект лиц.счета
                final String[] houseFIAS = {""};
                final String[] un = {""};
                if (eolink.isPresent()) {
                    eolinkMng.getEolinkByEolinkUpHierarchy(eolink.get(), "Дом").ifPresent(t -> {
                        houseFIAS[0] = t.getGuid();
                    });
                    un[0] = eolink.get().getUn();
                }
                for (Penya penya : kart.getPenya()) {
                    BigDecimal summDeb = Utl.nvl(penya.getSumma(), BigDecimal.ZERO);
                    BigDecimal summPen = Utl.nvl(penya.getPenya(), BigDecimal.ZERO);

                    if (summDeb.compareTo(BigDecimal.ZERO) != 0 || summPen.compareTo(BigDecimal.ZERO) != 0) {
                        // есть задолженность
                        debitRegistry.init();
                        debitRegistry.setDelimeter(";");
                        debitRegistry.addElem(
                                kart.getOwnerFIO(), // ФИО собственника
                                un[0], // ЕЛС
                                houseFIAS[0], // GUID дома по ФИАС
                                Utl.ltrim(kart.getNum(), "0"), // № квартиры
                                kartMng.getAdr(kart), // адрес
                                kart.getLsk(), // лиц.счет
                                summDeb.add(summPen).toString() // сумма задолженности с пенёй
                        );
                        debitRegistry.setDelimeter(";:[!]");
                        debitRegistry.addElem(Utl.getPeriodToMonthYear(penya.getMg1()));

                        if (kart.getTp().getCd().equals("LSK_TP_MAIN")
                                && penya.getMg1().equals(Utl.getPeriodFromDate(dt))) {
                            // счетчики, только для долгов текущего периода и только для основного лиц.счета
                            List<Meter> lstMeter = meterDAO.findActualByKo(kart.getKoKw().getId(), dt);
                            int i = 0;
                            for (Meter meter : lstMeter) {
                                i++;
                                if (meter.getN1() != null) {
                                    // если есть последние показания
                                    debitRegistry.setDelimeter(";");
                                    debitRegistry.addElem(meter.getId().toString());
                                    debitRegistry.addElem(meter.getUsl().getNm2());
                                    if (i == lstMeter.size()) {
                                        debitRegistry.setDelimeter(":[!]");
                                    }
                                    debitRegistry.addElem(meter.getN1().toString());
                                }
                            }
                        } else {
                            // долг прошлого периода - поставить флаг окончания блока счетчиков
                            debitRegistry.addElem(":[!]");
                        }
                        // основной долг
                        if (summDeb.compareTo(BigDecimal.ZERO) != 0) {
                            debitRegistry.setDelimeter(";");
                            // код услуги
                            debitRegistry.addElem(
                                    kartMng.generateUslNameShort(kart, 2, 3, "_"));
                            // наименование
                            debitRegistry.addElem(
                                    kartMng.generateUslNameShort(kart, 1, 3, ","));
                            // сумма к оплате
                            debitRegistry.addElem(summDeb.toString());
                        }
                        // пеня
                        if (summPen.compareTo(BigDecimal.ZERO) != 0) {
                            // код услуги
                            debitRegistry.addElem("PEN");
                            // наименование
                            debitRegistry.addElem("Пени");
                            // сумма к оплате
                            debitRegistry.addElem(summPen.toString());
                        }

                        // пустое поле
                        debitRegistry.addElem("");
                        String result = debitRegistry.getResult().toString();
                        //log.info(result);
                        writer.write(debitRegistry.getResult().toString() + "\n");
                    }
                }
            }
            log.info("Окончание формирования реестра задолженности по лиц.счетам для Сбербанка");
        } catch (IOException e) {
            log.error("ОШИБКА! Ошибка записи в файл c:\\temp\\reestr1.txt");
            e.printStackTrace();
        }

    }
}