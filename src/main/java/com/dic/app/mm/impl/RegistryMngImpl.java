package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.RegistryMng;
import com.dic.bill.dao.*;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.mm.impl.EolinkMngImpl;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
    private final MeterMng meterMng;
    private final KartDAO kartDAO;
    private final ConfigApp configApp;
    private final LoadKartExtDAO loadKartExtDAO;
    private final KartExtDAO kartExtDAO;
    private final HouseDAO houseDAO;


    public RegistryMngImpl(EntityManager em,
                           PenyaDAO penyaDAO, MeterDAO meterDAO, OrgDAO orgDAO, EolinkMng eolinkMng,
                           KartMng kartMng, MeterMng meterMng, KartDAO kartDAO, ConfigApp configApp,
                           LoadKartExtDAO loadKartExtDAO, KartExtDAO kartExtDAO, HouseDAO houseDAO) {
        this.em = em;
        this.penyaDAO = penyaDAO;
        this.meterDAO = meterDAO;
        this.orgDAO = orgDAO;
        this.eolinkMng = eolinkMng;
        this.kartMng = kartMng;
        this.meterMng = meterMng;
        this.kartDAO = kartDAO;
        this.configApp = configApp;
        this.loadKartExtDAO = loadKartExtDAO;
        this.kartExtDAO = kartExtDAO;
        this.houseDAO = houseDAO;
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
                                kart.getAdr(), // адрес
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

    private class KartExtInfo {
        String reu;
        String lskTp;
        House house;
        String kw;
        String extLsk;
        String address;
        Integer code;
        String guid;
        String fio;
        String nm;
        BigDecimal summa;
        Date dt;
        String periodDeb;
    }

    /**
     * Загрузить файл внешних лиц счетов во временную таблицу
     *
     * @param cityName - наименование города
     * @param reu      - код УК
     * @param lskTp    - тип лиц.счетов
     * @param fileName - путь и имя файла
     * @param codePage - кодовая страница
     * @return - кол-во успешно обработанных записей
     */
    @Override
    @Transactional
    public int loadFileKartExt(String cityName, String reu, String lskTp, String fileName, String codePage) throws FileNotFoundException {
        log.info("Начало загрузки файла внешних лиц.счетов fileName={}", fileName);
        Scanner scanner = new Scanner(new File(fileName), codePage);
        loadKartExtDAO.deleteAll();
        Set<String> setExt = new HashSet<>(); // уже обработанные внешние лиц.сч.
        List<House> lstHouse = houseDAO.findByGuidIsNotNull();
        Map<String, House> mapHouse = lstHouse.stream().collect(Collectors.toMap(House::getGuid, v -> v));
        int cntLoaded = 0;
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            //log.trace("s={}", s);
            int i = 0;
            Scanner sc = new Scanner(s);
            sc.useDelimiter(";");
            KartExtInfo kartExtInfo = new KartExtInfo();
            kartExtInfo.dt = new Date();
            kartExtInfo.reu = reu;
            kartExtInfo.lskTp = lskTp;
            // перебрать элементы строки
            boolean foundCity = false;
            while (sc.hasNext()) {
                i++;
                String elem = sc.next();
                if (i == 1) {
                    // внешний лиц.счет
                    kartExtInfo.extLsk = elem;
                } else if (i == 2) {
                    // GUID дома
                    //log.info("GUID={}", elem);
                    kartExtInfo.guid = elem;
                } else if (i == 3) {
                    // ФИО
                    kartExtInfo.fio = elem;
                } else if (i == 4) {
                    // город
                    Optional<String> city = getAddressElemByIdx(elem, ",", 0);
                    // поселок, если имеется
                    Optional<String> town = getAddressElemByIdx(elem, ",", 1);

                    // проверить найден ли нужный город
                    //city.ifPresent(t->log.info("city={}", t));
                    //town.ifPresent(t->log.info("town={}", t));
                    if (city.isPresent() && city.get().equals(cityName)) {
                        if (town.isPresent() && town.get().length() == 0) {
                            foundCity = true;
                            kartExtInfo.house = mapHouse.get(kartExtInfo.guid);
                        }
                    }
                    if (!foundCity) {
                        break;
                    }
                    kartExtInfo.address = elem;
                    // № помещения
                    getAddressElemByIdx(elem, ",", 4).ifPresent(t -> kartExtInfo.kw = t);
                } else if (i == 5) {
                    // код услуги
                    kartExtInfo.code = Integer.parseInt(elem);
                } else if (i == 6) {
                    // наименование услуги
                    kartExtInfo.nm = elem;
                } else if (i == 7) {
                    // период оплаты (задолженности)
                    kartExtInfo.periodDeb = elem;
                } else if (i == 8) {
                    // сумма задолженности
                    if (elem != null && elem.length() > 0) {
                        kartExtInfo.summa = new BigDecimal(elem);
                    }
                }
            }
            if (foundCity) {
                cntLoaded++;
                buildLoadKartExt(kartExtInfo, setExt);
            }
        }
        scanner.close();
        log.info("Окончание загрузки файла внешних лиц.счетов fileName={}, загружено {} строк", fileName, cntLoaded);
        return cntLoaded;
    }


    /**
     * Загрузить показания по счетчикам
     *
     * @param filePath         - имя файла, включая путь
     * @param codePage         - кодовая страница
     * @param isSetPreviousVal - установить предыдущее показание? ВНИМАНИЕ! Текущие введёные показания будут сброшены назад
     */
    @Override
    @Transactional
    public int loadFileMeterVal(String filePath, String codePage, boolean isSetPreviousVal) throws FileNotFoundException {
        log.info("Начало загрузки файла показаний по счетчикам filePath={}", filePath);
        String strPathBad = filePath.substring(0, filePath.length() - 4) + ".BAD";
        Path pathBad = Paths.get(strPathBad);
        Scanner scanner = new Scanner(new File(filePath), codePage);
        int cntLoaded = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(pathBad, Charset.forName("windows-1251"))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                line = line.replaceAll("\t", "");
                line = line.replaceAll("\"", "");
                // пропустить заголовок
                if (cntLoaded++ > 0) {
                    log.info("s={}", line);
                    // перебрать элементы строки
                    Scanner sc = new Scanner(line);
                    sc.useDelimiter(";");
                    String lsk = null;
                    int i = 0;
                    String strUsl = null;
                    String prevVal = null;
                    while (sc.hasNext()) {
                        i++;
                        String elem = sc.next();
                        if (i == 1) {
                            lsk = elem;
                        } else if (Utl.in(i, 3, 8, 13)) {
                            // услуга
                            strUsl = elem;
                        } else if (Utl.in(i, 4, 9, 14)) {
                            // установить предыдущие показания
                            prevVal = elem;
                        } else if (Utl.in(i, 5, 10, 15)) {
                            // отправить текущие показания
                            meterMng.sendMeterVal(writer, lsk, strUsl,
                                    prevVal, elem, configApp.getPeriod(), configApp.getCurUser().getId(), isSetPreviousVal);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        scanner.close();
        log.info("Окончание загрузки файла показаний по счетчикам filePath={}, загружено {} строк", filePath, cntLoaded);
        return cntLoaded;
    }

    /**
     * Выгрузить показания по счетчикам
     *
     * @param filePath - имя файла, включая путь
     * @param codePage - кодовая страница
     */
    @Override
    @Transactional
    public int unloadFileMeterVal(String filePath, String codePage, String strUk) throws IOException {
        log.info("Начало выгрузки файла показаний по счетчикам filePath={}, по УК={}", filePath, strUk);
        String[] parts = strUk.substring(1).split(";");
        String strPath;
        Date dt = new Date();
        Map<String, String> mapMeter = new HashMap<>();
        int cntLoaded = 0;
        for (String codeUk : parts) {
            codeUk = codeUk.replaceAll("'", "");
            log.info("Обрабатывается УК={}", codeUk);
            strPath = filePath + "_" + codeUk + ".csv";
            Path path = Paths.get(strPath);
            try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("windows-1251"))) {
                writer.write("\tЛиц.сч.;Адр.;Услуга;Показ.пред;Показ.тек.;Расход;\tЛиц.сч.;Услуга;Показ.пред;" +
                        "Показ.тек.;Расход;\tЛиц.сч.;Услуга;Показ.пред;Показ.тек.;Расход" + "\r\n");
                List<Kart> lstKart = kartDAO.findActualByUkOrderedByAddress(codeUk, "LSK_TP_MAIN");
                for (Kart kart : lstKart) {
                    cntLoaded++;
                    mapMeter.put("011", "Нет счетчика" + ";" + ";" + ";" + ";");
                    mapMeter.put("015", "Нет счетчика" + ";" + ";" + ";" + ";");
                    mapMeter.put("038", "Нет счетчика" + ";" + ";" + ";" + ";");
                    for (Meter meter : kart.getKoKw().getMeterByKo()) {
                        if (meter.getIsMeterActual(dt)) {
                            mapMeter.put(meter.getUsl().getId(),
                                    meter.getUsl().getId() + " " +
                                            meter.getUsl().getName().trim() + ";"
                                            + (meter.getN1() != null ? meter.getN1().toString() : "0")
                                            + ";" + ";" + ";"
                            );
                        }
                    }
                    writer.write("\t" + kart.getLsk() + ";" + kart.getAdr() + ";" + mapMeter.get("011") + "\t" + kart.getLsk() + ";"
                            + mapMeter.get("015") + "\t" + kart.getLsk() + ";" + mapMeter.get("038") + "\r\n");
                }
            }

            log.info("Окончание выгрузки файла показаний по счетчикам filePath={}, выгружено {} строк", filePath, cntLoaded);
        }
        return cntLoaded;
    }

    /**
     * Загрузить успешно обработанные лиц.счета в таблицу внешних лиц.счетов
     */
    @Override
    @Transactional
    public void loadApprovedKartExt() {
        List<LoadKartExt> lst = loadKartExtDAO.findApprovedForLoad();
        for (LoadKartExt loadKartExt : lst) {
            KartExt kartExt = KartExt.KartExtBuilder.aKartExt()
                    .withExtLsk(loadKartExt.getExtLsk())
                    .withKart(loadKartExt.getKart())
                    .withFio(loadKartExt.getFio())
                    .build();
            em.persist(kartExt);
        }
    }

    /**
     * Создать подготовительную запись внешнего лиц.счета
     *
     * @param kartExtInfo - информация для создания вн.лиц.счета
     * @param setExt      - уже обработанные вн.лиц.счета
     */
    private void buildLoadKartExt(KartExtInfo kartExtInfo, Set<String> setExt) {
        String comm = "";
        int status = 0;
        Kart kart = null;
        if (setExt.contains(kartExtInfo.extLsk)) {
            comm = "Дублируется внешний лиц.счет";
            status = 2;
        } else {
            setExt.add(kartExtInfo.extLsk);
            if (kartExtInfo.house != null) {
                List<Kart> lstKart;
                String strKw;
                if (kartExtInfo.kw != null && kartExtInfo.kw.length() > 0) {
                    // помещение
                    strKw = Utl.lpad(kartExtInfo.kw, "0", 7).toUpperCase();
                } else {
                    // нет помещения, частный дом?
                    strKw = "0000000";
                    comm = "Частный дом?";
                }

                lstKart = kartDAO.findActualByReuHouseIdTpKw(kartExtInfo.reu,
                        kartExtInfo.lskTp, kartExtInfo.house.getId(), strKw);
                if (lstKart.size() > 0) {
                    // взять первый лиц.счет в списке
                    kart = lstKart.get(0);
                } else {
                    comm = "Не найдено помещение с номером=" + strKw;
                    status = 2;
                }
                Optional<KartExt> kartExt = kartExtDAO.findByExtLsk(kartExtInfo.extLsk);
                if (kartExt.isPresent()) {
                    comm = "Уже загружен";
                    status = 1;
                }
            } else {
                comm = "Не найден дом с данным GUID в C_HOUSES!";
                status = 2;
            }
        }

        LoadKartExt loadKartExt =
                LoadKartExt.LoadKartExtBuilder.aLoadKartExt()
                        .withExtLsk(kartExtInfo.extLsk)
                        .withKart(kart)
                        .withAddress(kartExtInfo.address)
                        .withCode(kartExtInfo.code)
                        .withPeriodDeb(kartExtInfo.periodDeb)
                        .withGuid(kartExtInfo.guid)
                        .withFio(kartExtInfo.fio)
                        .withNm(kartExtInfo.nm)
                        .withSumma(kartExtInfo.summa)
                        .withComm(comm)
                        .withStatus(status)
                        .build();
        em.persist(loadKartExt);
    }

    /**
     * Получить элемент адреса по индексу
     *
     * @param address - адрес
     * @param elemIdx - индекс элемента
     */
    private Optional<String> getAddressElemByIdx(String address, String delimiter, Integer elemIdx) {
        Scanner scanner = new Scanner(address);
        scanner.useDelimiter(delimiter);
        int i = 0;
        while (scanner.hasNext()) {
            String adr = scanner.next();
            if (Utl.in(i++, elemIdx)) {
                return Optional.of(adr);
            }
        }
        return Optional.empty();
    }

}