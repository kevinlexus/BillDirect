package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.RegistryMng;
import com.dic.bill.dao.*;
import com.dic.bill.dto.KartExtPaymentRec;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.mm.impl.EolinkMngImpl;
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
    private final KwtpDAO kwtpDAO;
    private final AkwtpDAO akwtpDAO;
    private final HouseDAO houseDAO;


    public RegistryMngImpl(EntityManager em,
                           PenyaDAO penyaDAO, MeterDAO meterDAO, OrgDAO orgDAO, EolinkMng eolinkMng,
                           KartMng kartMng, MeterMng meterMng, KartDAO kartDAO, ConfigApp configApp,
                           LoadKartExtDAO loadKartExtDAO, KartExtDAO kartExtDAO, KwtpDAO kwtpDAO,
                           AkwtpDAO akwtpDAO, HouseDAO houseDAO) {
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
        this.kwtpDAO = kwtpDAO;
        this.akwtpDAO = akwtpDAO;
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
        lstOrg.stream().filter(t -> t.getReu() != null && t.getGrpDeb() == null).forEach(this::genDebitForSberbankByReu);
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
    private void genDebitForSberbankByReu(Org uk) {
        // префикс для файла
        String prefix = uk.getReu();
        List<Kart> lstKart = penyaDAO.getKartWithDebitByReu(uk.getId());
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
        List<Kart> lstKart = penyaDAO.getKartWithDebitByGrpDeb(grpDeb);
        genDebitForSberbankVar1(prefix, lstKart);
    }

    /**
     * Сформировать реестр задолженности по лиц.счетам для Сбербанка (Кис, Полыс)
     *
     * @param prefix  - наименование префикса для файла
     * @param lstKart - список лиц.счетов
     */
    private void genDebitForSberbankVar1(String prefix, List<Kart> lstKart) {
        String strPath = "c:\\temp\\dolg\\dolg_" + prefix + ".txt";
        log.info("Формирование реестра задолженности в файл: {}", strPath);
        Path path = Paths.get(strPath);
        BigDecimal amount = BigDecimal.ZERO;
        BigDecimal amountWithEls = BigDecimal.ZERO;
        String period = configApp.getPeriod();
        Map<String, DebitRegistryEls> mapDebitReg = new HashMap<>();
        int cnt = 0;
        int cntWithEls = 0;
        DebitRegistryRec debitRegistryRec = new DebitRegistryRec();
        try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("windows-1251"))) {
            for (Kart kart : lstKart) {
                EolinkMngImpl.EolinkParams eolinkParams = eolinkMng.getEolinkParamsOfKartMain(kart);
                // суммировать долг по лиц.счету
                BigDecimal summDeb = BigDecimal.ZERO;
                BigDecimal summPen = BigDecimal.ZERO;
                for (Penya penya : kart.getPenya()) {
                    summDeb = summDeb.add(Utl.nvl(penya.getSumma(), BigDecimal.ZERO));
                    summPen = summPen.add(Utl.nvl(penya.getPenya(), BigDecimal.ZERO));
                }

                if (!kart.getStatus().getCd().equals("NLIV") &&
                        (kart.isActual() ||
                                (summDeb.add(summPen).compareTo(BigDecimal.ZERO) != 0))) {
                    // либо открытый лиц.счет либо есть задолженность (переплата)
                    amount = amount.add(summDeb).add(summPen);
                    cnt++;
                    // есть задолженность
                    debitRegistryRec.init();
                    debitRegistryRec.setDelimeter("|");
                    debitRegistryRec.addElem(
                            kart.getLsk() // лиц.счет
                    );
                    String key = null;
                    String els = null;
                    String houseGUID = null;
                    String kw = null;
                    if (eolinkParams.getHouseGUID().length() > 0 || eolinkParams.getUn().length() > 0) {
                        if (eolinkParams.getUn() != null && eolinkParams.getUn().length() > 0) {
                            els = eolinkParams.getUn();
                            amountWithEls = amountWithEls.add(summDeb).add(summPen);
                            cntWithEls++;
                        }
                        houseGUID = eolinkParams.getHouseGUID();
                        kw = Utl.ltrim(kart.getNum(), "0");
                        debitRegistryRec.addElem(els); // ЕЛС
                        debitRegistryRec.setDelimeter(",");
                        debitRegistryRec.addElem(houseGUID); // GUID дома
                        debitRegistryRec.setDelimeter("|");
                        debitRegistryRec.addElem(kw); // № квартиры
                        key = els;
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
                    BigDecimal summAmnt = summDeb.add(summPen).multiply(BigDecimal.valueOf(100))
                            .setScale(0, BigDecimal.ROUND_HALF_UP);
                    debitRegistryRec.addElem(summAmnt
                            .toString() // сумма задолженности с пенёй в копейках
                    );

                    if (key != null) {
                        key += period;
                        DebitRegistryEls prevRec = mapDebitReg.putIfAbsent(key,
                                new DebitRegistryEls(els, houseGUID, kw, kart.getOwnerFIO(),
                                        kartMng.getAdrWithCity(kart),
                                        kart.getUk().getName(),
                                        period, summAmnt));
                        if (prevRec != null) {
                            prevRec.setDeb(prevRec.getDeb().add(summAmnt));
                        }
                    }

                    String result = debitRegistryRec.getResult().toString();
                    log.trace(result);
                    writer.write(debitRegistryRec.getResult().toString() + "\r\n");
                }
            }
            // итоговый маркер
            writeAmountMark(amount, cnt, debitRegistryRec, writer);

        } catch (IOException e) {
            log.error("ОШИБКА! Ошибка записи в файл {}", strPath);
            e.printStackTrace();
        }


        // запись сгруппированного по ЕЛС файла долгов
        strPath = "c:\\temp\\dolg\\dolg_ELS_" + prefix + ".txt";
        log.info("Формирование реестра задолженности по ЕЛС в файл: {}", strPath);
        path = Paths.get(strPath);

        try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("windows-1251"))) {
            for (Map.Entry<String, DebitRegistryEls> entry : mapDebitReg.entrySet()) {
                DebitRegistryEls value = entry.getValue();
                debitRegistryRec.init();
                debitRegistryRec.setDelimeter("|");
                debitRegistryRec.addElem(value.getEls()); // ЕЛС
                debitRegistryRec.setDelimeter("|");
                debitRegistryRec.addElem(value.getEls()); // ЕЛС второй раз, в соответствии с форматом
                debitRegistryRec.setDelimeter(",");
                debitRegistryRec.addElem(value.getHouseGUID()); // GUID дома
                debitRegistryRec.setDelimeter("|");
                debitRegistryRec.addElem(value.getKw()); // № квартиры
                debitRegistryRec.addElem(value.getFio(),// ФИО собственника
                        value.getAdr(),// адрес
                        "1", // тип услуги
                        "Квартплата " + value.getUkName(), // УК
                        value.getPeriod(), // период
                        "" // пустое поле
                );
                debitRegistryRec.setDelimeter("");
                debitRegistryRec.addElem(value.getDeb().toString());
                String result = debitRegistryRec.getResult().toString();
                log.trace(result);
                writer.write(debitRegistryRec.getResult().toString() + "\r\n");
            }
            // итоговый маркер
            writeAmountMark(amountWithEls, cntWithEls, debitRegistryRec, writer);

        } catch (IOException e) {
            log.error("ОШИБКА! Ошибка записи в файл {}", strPath);
            e.printStackTrace();
        }

    }

    private void writeAmountMark(BigDecimal amount, int i, DebitRegistryRec debitRegistryRec, BufferedWriter writer) throws IOException {
        debitRegistryRec.init();
        debitRegistryRec.setDelimeter("|");
        debitRegistryRec.addElem("=");
        debitRegistryRec.addElem(String.valueOf(i));
        debitRegistryRec.setDelimeter("");
        debitRegistryRec.addElem(amount.setScale(0, BigDecimal.ROUND_HALF_UP).toString());
        writer.write(debitRegistryRec.getResult().toString() + "\r\n");
    }

    private class KartExtInfo {
        String reu;
        String lskTp;
        House house;
        String kw;
        String uslId;
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
     * @param uslId    - код услуги
     * @param lskTp    - тип лиц.счетов
     * @param fileName - путь и имя файла
     * @return - кол-во успешно обработанных записей
     */
    @Override
    @Transactional
    public int loadFileKartExt(String cityName, String reu, String uslId, String lskTp, String fileName) throws FileNotFoundException {
        log.info("Начало загрузки файла внешних лиц.счетов fileName={}", fileName);
        Scanner scanner = new Scanner(new File(fileName), "windows-1251");
        loadKartExtDAO.deleteAll();
        Set<String> setExt = new HashSet<>(); // уже обработанные внешние лиц.сч.
        List<House> lstHouse = houseDAO.findByGuidIsNotNull();
        lstHouse.forEach(t -> log.info("House.id={}, House.guid={}", t.getId(), t.getGuid()));
        Map<String, House> mapHouse = lstHouse.stream().collect(Collectors.toMap(House::getGuid, v -> v));
        int cntLoaded = 0;
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            //log.trace("s={}", s);
            int i = 0;
            int j = 0;
            Scanner sc = new Scanner(s);
            sc.useDelimiter(";");
            KartExtInfo kartExtInfo = new KartExtInfo();
            kartExtInfo.dt = new Date();
            kartExtInfo.reu = reu;
            kartExtInfo.uslId = uslId;
            kartExtInfo.lskTp = lskTp;
            // перебрать элементы строки
            boolean foundCity = false;
            while (sc.hasNext()) {
                i++;
                j++;
                if (j > 100) {
                    j = 1;
                    log.info("Обработано {} записей", i);
                }
                String elem = sc.next();
                log.info("elem={}", elem);
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
                    // поселок, если имеется todo временно убрал посёлок - разобраться потом ред.17.04.20
                    // Optional<String> town = getAddressElemByIdx(elem, ",", 1);

                    // проверить найден ли нужный город
                    //city.ifPresent(t->log.info("city={}", t));
                    //town.ifPresent(t->log.info("town={}", t));
                    if (city.isPresent() && city.get().equals(cityName)) {
                        //if (town.isPresent() && town.get().length() == 0) { todo временно убрал посёлок - разобраться потом ред.17.04.20
                        foundCity = true;
                        kartExtInfo.house = mapHouse.get(kartExtInfo.guid);
                        if (kartExtInfo.house == null) {
                            log.error("Не найден дом по guid={}", kartExtInfo.guid);
                        }
                        //}
                    } else {
                        if (city.isPresent()) {
                            //log.error("Наименование города={} не соответстует ключевому наименованию города={}",
                            //        city.get(), cityName);
                        } else {
                            log.error("Наименование города {} не получено из строки файла", cityName);
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
                createLoadKartExt(kartExtInfo, setExt);
            }
        }
        scanner.close();
        log.info("Окончание загрузки файла внешних лиц.счетов fileName={}, загружено {} строк", fileName, cntLoaded);
        return cntLoaded;
    }

    /**
     * Выгрузить файл платежей по внешними лиц.счетами
     *
     * @param filePath - имя файла, включая путь
     * @param reu      - код УК
     * @param genDt1   - начало периода
     * @param genDt2   - окончание периода
     */
    @Override
    @Transactional
    public int unloadPaymentFileKartExt(String filePath, String reu, Date genDt1, Date genDt2) throws IOException {
        Org uk = orgDAO.getByReu(reu);
        log.info("Начало выгрузки файла платежей по внешним лиц.счетам filePath={}, " +
                "по УК={}-{}, genDt1={} genDt2={}", filePath, reu, uk.getName(), genDt1, genDt2);
        Path path = Paths.get(filePath);
        // внешние лиц.счета привязаны через LSK
        String period = Utl.getPeriodFromDate(genDt1);
        List<KartExtPaymentRec> payment = akwtpDAO.getPaymentByPeriod(period, uk.getId());
        int cntLoaded = 0;
        BigDecimal amount = BigDecimal.ZERO;
        if (payment.size() > 0) {
            try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("windows-1251"))) {
                for (KartExtPaymentRec rec : payment) {
                writer.write(Utl.getStrFromDate(rec.getDt(), "ddMMyyyy") + ";" +
                        rec.getExtLsk() + ";1;" + rec.getSumma().toString() + ";" +
                        rec.getId() + "\r\n");
                amount = amount.add(rec.getSumma());
                cntLoaded++;
                }
                writer.write("=;" + cntLoaded + ";" + amount.toString());
            }
        }
        log.info("Окончание выгрузки файла платежей по внешним лиц.счетам filePath={}, выгружено {} строк", filePath, cntLoaded);
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
        Doc doc = Doc.DocParBuilder.aDocPar().withTuser(configApp.getCurUser()).build();
        doc.setComm("файл: " + filePath);
        doc.setIsSetPreviousVal(isSetPreviousVal);
        doc.setMg(configApp.getPeriod());
        em.persist(doc); // persist - так как получаем Id // note Используй crud.save
        em.flush(); // сохранить запись Doc до вызова процедуры, иначе не найдет foreign key
        doc.setCd("Registry_Meter_val_" + Utl.getStrFromDate(new Date()) + "_" + doc.getId());
        log.info("Начало загрузки файла показаний по счетчикам filePath={} CD={}", filePath, doc.getCd());
        String strPathBad = filePath.substring(0, filePath.length() - 4) + ".BAD";
        Path pathBad = Paths.get(strPathBad);
        Scanner scanner = new Scanner(new File(filePath), codePage);
        int cntRec = 0;
        int cntLoaded = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(pathBad, Charset.forName("windows-1251"))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                line = line.replaceAll("\t", "");
                line = line.replaceAll("\"", "");
                // пропустить заголовок
                if (cntRec++ > 0) {
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
                            int ret = meterMng.sendMeterVal(writer, lsk, strUsl,
                                    prevVal, elem, configApp.getPeriod(), configApp.getCurUser().getId(),
                                    doc.getId(), isSetPreviousVal);
                            if (ret == 0) {
                                cntLoaded++;
                            }
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
        String[] parts = strUk.substring(1).split(";");
        String strPath;
        Date dt = new Date();
        Map<String, String> mapMeter = new HashMap<>();
        int cntLoaded = 0;
        for (String reu : parts) {
            reu = reu.replaceAll("'", "");
            Org uk = orgDAO.getByReu(reu);
            strPath = filePath + "_" + reu + ".csv";
            log.info("Начало выгрузки файла показаний по счетчикам filePath={}, по УК={}-{}", filePath, strUk, uk.getName());
            Path path = Paths.get(strPath);
            try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("windows-1251"))) {
                writer.write("\tЛиц.сч.;Адр.;Услуга;Показ.пред;Показ.тек.;Расход;\tЛиц.сч.;Услуга;Показ.пред;" +
                        "Показ.тек.;Расход;\tЛиц.сч.;Услуга;Показ.пред;Показ.тек.;Расход" + "\r\n");
                List<Kart> lstKart = kartDAO.findActualByReuStatusOrderedByAddress(reu, Arrays.asList("PRV", "MUN"),
                        uk.isRSO() ? "LSK_TP_RSO" : "LSK_TP_MAIN");
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
                    .withKoKw(loadKartExt.getKoKw())
                    .withKoPremise(loadKartExt.getKoPremise())
                    .withFio(loadKartExt.getFio())
                    .withV(1)
                    .build();
            em.persist(kartExt); // note Используй crud.save
        }
    }

    /**
     * Создать подготовительную запись внешнего лиц.счета
     *
     * @param kartExtInfo - информация для создания вн.лиц.счета
     * @param setExt      - уже обработанные вн.лиц.счета
     */
    private void createLoadKartExt(KartExtInfo kartExtInfo, Set<String> setExt) {
        String comm = "";
        int status = 0;
        Kart kart = null;
        if (setExt.contains(kartExtInfo.extLsk)) {
            comm = "В файле дублируется внешний лиц.счет";
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

                Optional<KartExt> kartExt = kartExtDAO.findByExtLsk(kartExtInfo.extLsk);
                if (kartExt.isPresent()) {
                    comm = "Внешний лиц.счет уже создан";
                    status = 1;
                    log.info("Внешний лиц.счет уже создан");
                } else {
                    // поиск по kart.reu
                    log.info("поиск по kart.reu: reu={}, lskTp={}, house={}, strKw={}",
                            kartExtInfo.reu,
                            kartExtInfo.lskTp, kartExtInfo.house.getId(), strKw);
                    lstKart = kartDAO.findActualByReuHouseIdTpKw(kartExtInfo.reu,
                            kartExtInfo.lskTp, kartExtInfo.house.getId(), strKw);
                    if (lstKart.size() == 0) {
                        // поиск по nabor.usl, если не найдено по kart.reu
                        log.info("поиск по nabor.usl: lskTp={}, uslId={}, houseId={}, strKw={}",
                                kartExtInfo.lskTp, kartExtInfo.uslId, kartExtInfo.house.getId(), strKw);
                        lstKart = kartDAO.findActualByUslHouseIdTpKw(
                                kartExtInfo.lskTp, kartExtInfo.uslId, kartExtInfo.house.getId(), strKw);
                    }

                    if (lstKart.size() == 1) {
                        kart = lstKart.get(0);
                    } else if (lstKart.size() > 1) {
                        // могут быть два и больше открытых лиц.счета - поделены судом,
                        // в таком случае пользователь сам редактирует поле фин.лиц.счета и делает статус = 0
                        comm = "Присутствуют более одного открытого лиц.сч. по данному адресу, " +
                                "необходимо определить K_LSK_ID вручную и поставить статус=0";
                        status = 4;
                    } else {
                        comm = "Не найдено помещение с номером=" + strKw;
                        status = 2;
                    }

                    // проверка на существование внешнего лиц.счета по данному адресу
                    List<KartExt> lstKartExt = kartExtDAO.getKartExtByHouseIdAndKw(kartExtInfo.house.getId(), strKw);
                    if (lstKartExt.size() > 0) {
                        comm = "Внешний лиц.счет по данному адресу уже существует (" +
                                lstKartExt.get(0).getExtLsk() + "), возможно его необходимо закрыть?";
                        status = 2;
                    }

                }
            } else {
                comm = "Не найден дом с данным GUID в C_HOUSES!";
                status = 3;
            }
        }

        LoadKartExt loadKartExt;
        if (kart != null) {
            loadKartExt =
                    LoadKartExt.LoadKartExtBuilder.aLoadKartExt()
                            .withExtLsk(kartExtInfo.extLsk)
                            .withKoKw(kart.getKoKw())
                            .withKoPremise(kart.getKoPremise())
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
        } else {
            loadKartExt =
                    LoadKartExt.LoadKartExtBuilder.aLoadKartExt()
                            .withExtLsk(kartExtInfo.extLsk)
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
        }
        em.persist(loadKartExt); // note Используй crud.save
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