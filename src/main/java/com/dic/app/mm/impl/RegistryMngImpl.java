package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.RegistryMng;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.OrgDAO;
import com.dic.bill.dao.PenyaDAO;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.impl.EolinkMngImpl;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Meter;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Penya;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Optional;

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
    private final OrgDAO orgDAO;
    private final EolinkMng eolinkMng;
    private final KartMng kartMng;
    private final ConfigApp configApp;


    public RegistryMngImpl(EntityManager em,
                           PenyaDAO penyaDAO, MeterDAO meterDAO, OrgDAO orgDAO, EolinkMng eolinkMng, KartMng kartMng, ConfigApp configApp) {
        this.em = em;
        this.penyaDAO = penyaDAO;
        this.meterDAO = meterDAO;
        this.orgDAO = orgDAO;
        this.eolinkMng = eolinkMng;
        this.kartMng = kartMng;
        this.configApp = configApp;
    }

    /**
     * Сформировать реест задолженности по лиц.счетам для Сбербанка
     */
    @Override // метод readOnly - иначе вызывается масса hibernate.AutoFlush - тормозит в Полыс, ред.04.09.2019
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, rollbackFor = Exception.class)
    public void genDebitForSberbank() {
        log.info("Начало формирования реестра задолженности по лиц.счетам для Сбербанка");
        List<Org> lstOrg = orgDAO.findAll();
        // формировать задолженность по УК
        lstOrg.stream().filter(t -> t.getReu() != null && t.getGrpDeb() == null).forEach(this::genDebitForSberbankByUk);
        // формировать задолженность по группам
        lstOrg.stream().filter(t -> t.getReu() != null && t.getGrpDeb() != null).map(Org::getGrpDeb).distinct()
                .forEach(this::genDebitForSberbankByGrpDeb);
        log.info("Окончание формирования реестра задолженности по лиц.счетам для Сбербанка");
    }

    /**
     * Сформировать реест задолженности по лиц.счетам для Сбербанка по УК
     *
     * @param uk - УК
     */
    private void genDebitForSberbankByUk(Org uk) {
        // префикс для файла
        String prefix = uk.getReu();
        List<Kart> lstKart = penyaDAO.getKartWhereDebitExistsByUk(uk.getId());
        genDebitForSberbankVar1(prefix, lstKart);
    }

    /**
     * Сформировать реест задолженности по лиц.счетам для Сбербанка по группе задолженности
     *
     * @param grpDeb - группировка для долгов Сбера
     */
    private void genDebitForSberbankByGrpDeb(int grpDeb) {
        // префикс для файла
        String prefix = String.valueOf(grpDeb);
        List<Kart> lstKart = penyaDAO.getKartWhereDebitExistsByGrpDeb(grpDeb);
        genDebitForSberbankVar1(prefix, lstKart);
    }

    /**
     * Сформировать реестр задолженности по лиц.счетам для Сбербанка
     *
     * @param prefix  - наименование префикса для файла
     * @param lstKart - список лиц.счетов
     */
    private void genDebitForSberbankVar1(String prefix, List<Kart> lstKart) {
        String strPath = "c:\\temp\\dolg\\dolg_" + prefix + ".txt";
        log.info("Формирование реестра задолженности в файл: {}", strPath);
        Path path = Paths.get(strPath);
        BigDecimal amount = BigDecimal.ZERO;
        String period = configApp.getPeriod();
        int i = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("windows-1251"))) {
            DebitRegistryRec debitRegistryRec = new DebitRegistryRec();
            for (Kart kart : lstKart) {
                Optional<Eolink> eolink = kart.getEolink().stream().findFirst();
                EolinkMngImpl.EolinkParams eolinkParams = eolinkMng.getActualEolinkParams(eolink, kart);
                // суммировать долг по лиц.счету
                BigDecimal summDeb = BigDecimal.ZERO;
                BigDecimal summPen = BigDecimal.ZERO;
                for (Penya penya : kart.getPenya()) {
                    summDeb = summDeb.add(Utl.nvl(penya.getSumma(), BigDecimal.ZERO));
                    summPen = summPen.add(Utl.nvl(penya.getPenya(), BigDecimal.ZERO));
                }

                if (kart.isActual() || (summDeb.compareTo(BigDecimal.ZERO) != 0 || summPen.compareTo(BigDecimal.ZERO) != 0)) {
                    // либо открытый лиц.счет либо есть задолженность
                    amount = amount.add(summDeb).add(summPen);
                    i++;
                    // есть задолженность
                    debitRegistryRec.init();
                    debitRegistryRec.setDelimeter("|");
                    debitRegistryRec.addElem(
                            kart.getLsk() // лиц.счет
                    );
                    if (eolinkParams.getHouseGUID().length() > 0 || eolinkParams.getUn().length() > 0) {
                        debitRegistryRec.addElem(eolinkParams.getUn()); // ЕЛС
                        debitRegistryRec.setDelimeter(",");
                        debitRegistryRec.addElem(eolinkParams.getHouseGUID()); // GUID дома
                        debitRegistryRec.setDelimeter("|");
                        debitRegistryRec.addElem(Utl.ltrim(kart.getNum(), "0")); // № квартиры
                    } else {
                        // нет ЕЛС или GUID дома,- поставить два пустых элемента
                        debitRegistryRec.addElem("");
                        debitRegistryRec.addElem("");
                    }
                    debitRegistryRec.addElem(kart.getOwnerFIO(), // ФИО собственника
                            kartMng.getAdrWithCity(kart), // адрес
                            "1", // тип услуги
                            "Квартплата " + kart.getUk().getName(), // УК
                            Utl.getPeriodToMonthYear(period), // период
                            "" // пустое поле
                    );
                    debitRegistryRec.setDelimeter("");
                    debitRegistryRec.addElem(
                            summDeb.add(summPen).multiply(BigDecimal.valueOf(100))
                                    .setScale(0, BigDecimal.ROUND_HALF_UP).toString() // сумма задолженности с пенёй в копейках
                    );

                    String result = debitRegistryRec.getResult().toString();
                    log.info(result);
                    writer.write(debitRegistryRec.getResult().toString() + "\r\n");
                }
            }
            // итоговый маркер
            debitRegistryRec.init();
            debitRegistryRec.setDelimeter("|");
            debitRegistryRec.addElem("=");
            debitRegistryRec.addElem(String.valueOf(i));
            debitRegistryRec.setDelimeter("");
            debitRegistryRec.addElem(amount.setScale(0, BigDecimal.ROUND_HALF_UP).toString());
            writer.write(debitRegistryRec.getResult().toString() + "\r\n");

        } catch (IOException e) {
            log.error("ОШИБКА! Ошибка записи в файл {}", strPath);
            e.printStackTrace();
        }

    }

    /**
     * Сформировать реестр задолженности по лиц.счетам для Сбербанка
     *
     * @param prefix  - наименование префикса для файла
     * @param lstKart - список лиц.счетов
     */
    private void genDebitForSberbankVar2(String prefix, List<Kart> lstKart) {
        Path path = Paths.get("c:\\temp\\dolg\\dolg_" + prefix + ".txt");
        // дата формирования
        Date dt = new Date();
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            DebitRegistryRec debitRegistryRec = new DebitRegistryRec();
            for (Kart kart : lstKart) {
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
                        debitRegistryRec.init();
                        debitRegistryRec.setDelimeter(";");
                        debitRegistryRec.addElem(
                                kart.getOwnerFIO(), // ФИО собственника
                                un[0], // ЕЛС
                                houseFIAS[0], // GUID дома по ФИАС
                                Utl.ltrim(kart.getNum(), "0"), // № квартиры
                                kartMng.getAdr(kart), // адрес
                                kart.getLsk(), // лиц.счет
                                summDeb.add(summPen).toString() // сумма задолженности с пенёй
                        );
                        debitRegistryRec.setDelimeter(";:[!]");
                        debitRegistryRec.addElem(Utl.getPeriodToMonthYear(penya.getMg1()));

                        if (kart.getTp().getCd().equals("LSK_TP_MAIN")
                                && penya.getMg1().equals(Utl.getPeriodFromDate(dt))) {
                            // счетчики, только для долгов текущего периода и только для основного лиц.счета
                            List<Meter> lstMeter = meterDAO.findActualByKo(kart.getKoKw().getId(), dt);
                            int i = 0;
                            for (Meter meter : lstMeter) {
                                i++;
                                if (meter.getN1() != null) {
                                    // если есть последние показания
                                    debitRegistryRec.setDelimeter(";");
                                    debitRegistryRec.addElem(meter.getId().toString());
                                    debitRegistryRec.addElem(meter.getUsl().getNm2());
                                    if (i == lstMeter.size()) {
                                        debitRegistryRec.setDelimeter(":[!]");
                                    }
                                    debitRegistryRec.addElem(meter.getN1().toString());
                                }
                            }
                        } else {
                            // долг прошлого периода - поставить флаг окончания блока счетчиков
                            debitRegistryRec.addElem(":[!]");
                        }
                        // основной долг
                        if (summDeb.compareTo(BigDecimal.ZERO) != 0) {
                            debitRegistryRec.setDelimeter(";");
                            // код услуги
                            debitRegistryRec.addElem(
                                    kartMng.generateUslNameShort(kart, 2, 3, "_"));
                            // наименование
                            debitRegistryRec.addElem(
                                    kartMng.generateUslNameShort(kart, 1, 3, ","));
                            // сумма к оплате
                            debitRegistryRec.addElem(summDeb.toString());
                        }
                        // пеня
                        if (summPen.compareTo(BigDecimal.ZERO) != 0) {
                            // код услуги
                            debitRegistryRec.addElem("PEN");
                            // наименование
                            debitRegistryRec.addElem("Пени");
                            // сумма к оплате
                            debitRegistryRec.addElem(summPen.toString());
                        }

                        // пустое поле
                        debitRegistryRec.addElem("");
                        String result = debitRegistryRec.getResult().toString();
                        //log.info(result);
                        writer.write(debitRegistryRec.getResult().toString() + "\n");
                    }
                }
            }
        } catch (IOException e) {
            log.error("ОШИБКА! Ошибка записи в файл c:\\temp\\reestr1.txt");
            e.printStackTrace();
        }

    }
}