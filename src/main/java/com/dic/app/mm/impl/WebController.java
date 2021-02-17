package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.dao.OrgDAO;
import com.dic.bill.dao.PrepErrDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.dto.KwtpMgRec;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.NaborMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Контроллер WEB - запросов
 */
@RestController
@Slf4j
public class WebController implements CommonConstants {

    @PersistenceContext
    private EntityManager em;

    private final ProcessMng processMng;
    private final NaborMng naborMng;
    private final MigrateMng migrateMng;
    private final ExecMng execMng;
    private final SprGenItmDAO sprGenItmDao;
    private final PrepErrDAO prepErrDao;
    private final OrgDAO orgDAO;
    private final ConfigApp config;
    private final ApplicationContext ctx;
    private final DistPayMng distPayMng;
    private final DistPayQueueMng distPayQueueMng;
    private final CorrectsMng correctsMng;
    private final RegistryMng registryMng;
    private final KartMng kartMng;
    private final MntBase mntBase;

    public WebController(NaborMng naborMng, MigrateMng migrateMng, ExecMng execMng,
                         SprGenItmDAO sprGenItmDao, PrepErrDAO prepErrDao,
                         OrgDAO orgDAO, ConfigApp config, ApplicationContext ctx,
                         DistPayMng distPayMng,
                         DistPayQueueMng distPayQueueMng,
                         ProcessMng processMng,
                         CorrectsMng correctsMng, RegistryMng registryMng, KartMng kartMng, MntBase mntBase) {
        this.naborMng = naborMng;
        this.migrateMng = migrateMng;
        this.execMng = execMng;
        this.sprGenItmDao = sprGenItmDao;
        this.prepErrDao = prepErrDao;
        this.orgDAO = orgDAO;
        this.distPayMng = distPayMng;
        this.distPayQueueMng = distPayQueueMng;
        this.config = config;
        this.ctx = ctx;
        this.processMng = processMng;
        this.correctsMng = correctsMng;
        this.registryMng = registryMng;
        this.kartMng = kartMng;
        this.mntBase = mntBase;
    }

    /**
     * Корректировочная проводка по сальдо
     *
     * @param var   - вариант проводки (1- распр.кредит по дебету по выбранным орг. Кис. выполняется после 15 числа
     *              2 - распр.кредит по 003 орг - выполняется 31 числа или позже, до перехода)
     * @param strDt - дата корректировки (T_CORRECTS_PAYMENTS.DAT)
     */
    @RequestMapping("/correct")
    public String correct(
            @RequestParam(value = "var") int var,
            @RequestParam(value = "strDt") String strDt,
            @RequestParam(value = "uk") String uk) {
        log.info("GOT /correct with: var={}, strDt={}, uk={}",
                var, strDt, uk);
        try {
            correctsMng.corrPayByCreditSal(var, Utl.getDateFromStr(strDt), uk);
        } catch (ParseException | WrongParam e) {
            log.error(Utl.getStackTraceString(e));
            return "ERROR";
        }
        return "OK";
    }

    /**
     * Распределить платеж C_KWTP_MG
     *
     * @param kwtpMgId - ID записи C_KWTP_MG
     * @param lsk      - лиц.счет
     * @param strSumma - сумма оплаты долга
     * @param strPenya - сумма оплаты пени
     * @param dopl     - период оплаты
     * @param nink     - № инкассации
     * @param nkom     - № компьютера
     * @param oper     - код операции
     * @param strDtek  - дата платежа
     * @param strDtInk - дата инкассации
     */
    @RequestMapping("/distKwtpMg")
    public String distKwtpMg(
            @RequestParam(value = "kwtpMgId") int kwtpMgId,
            @RequestParam(value = "lsk") String lsk,
            @RequestParam(value = "strSumma") String strSumma,
            @RequestParam(value = "strPenya") String strPenya,
            @RequestParam(value = "strDebt") String strDebt,
            @RequestParam(value = "dopl") String dopl,
            @RequestParam(value = "nink") int nink,
            @RequestParam(value = "nkom") String nkom,
            @RequestParam(value = "oper") String oper,
            @RequestParam(value = "strDtek") String strDtek,
            @RequestParam(value = "strDtInk") String strDtInk,
            @RequestParam(value = "useQueue") int useQueue
    ) {
        log.info("GOT /distKwtpMg with: kwtpMgId={}, lsk={}, strSumma={}, " +
                        "strPenya={}, strDebt={}, dopl={}, nink={}, nkom={}, oper={}, strDtek={}, strDtInk={}," +
                        "useQueue={}",
                kwtpMgId, lsk, strSumma, strPenya, strDebt, dopl, nink, nkom, oper, strDtek, strDtInk, useQueue);
        if (useQueue == 1) {
            // использовать очередь (асинхронно)
            distPayQueueMng.queueKwtpMg(new KwtpMgRec(kwtpMgId, lsk, strSumma, strPenya, strDebt,
                    dopl, nink, nkom, oper, strDtek, strDtInk, false));
            log.info("Поставлен в очередь на распределение платеж kwtpMg.id={}", kwtpMgId);
        } else {
            // распределить сразу
            try {
                distPayMng.distKwtpMg(kwtpMgId, lsk, strSumma, strPenya, strDebt,
                        dopl, nink, nkom, oper, strDtek, strDtInk, false);
                log.info("Распределён платеж kwtpMg.id={}", kwtpMgId);
            } catch (ErrorWhileDistPay e) {
                log.error(Utl.getStackTraceString(e));
                return "ERROR";
            }
        }
        return "OK";
    }

    /**
     * Расчет
     *
     * @param tp       - тип выполнения 0-начисление, 1-задолженность и пеня, 2 - распределение объемов по вводу,
     *                 4 - начисление по одной услуге, для автоначисления
     * @param houseId  - houseId объекта (дом)
     * @param vvodId   - vvodId объекта (ввод)
     * @param klskId   - klskId объекта (помещение)
     * @param debugLvl - уровень отладки 0, null - не записивать в лог отладочную информацию, 1 - записывать
     * @param genDtStr - дата на которую сформировать
     * @param stop     - 1 - остановить выполнение текущей операции с типом tp
     */
    @CacheEvict(value = {"KartMng.getKartMainLsk", // чистить кэш каждый раз, перед выполнением
            "PriceMng.multiplyPrice",
            "ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    @RequestMapping("/gen")
    public String gen(
            @RequestParam(value = "tp", defaultValue = "0") int tp,
            @RequestParam(value = "houseId", defaultValue = "0", required = false) int houseId,
            @RequestParam(value = "vvodId", defaultValue = "0", required = false) long vvodId,
            @RequestParam(value = "klskId", defaultValue = "0", required = false) long klskId,
            @RequestParam(value = "debugLvl", defaultValue = "0") int debugLvl,
            @RequestParam(value = "uslId", required = false) String uslId,
            @RequestParam(value = "reuId", required = false) String reuId,
            @RequestParam(value = "genDt", defaultValue = "") String genDtStr,
            @RequestParam(value = "stop", defaultValue = "0", required = false) int stop
    ) {
        log.info("GOT /gen with: tp={}, debugLvl={}, genDt={}, reuId={}, houseId={}, vvodId={}, klskId={}, uslId={}, stop={}",
                tp, debugLvl, genDtStr, reuId, houseId, vvodId, klskId, uslId, stop);
        String retStatus;
        if (stop == 1) {
            // Остановка всех процессов (отмена формирования например)
            config.getLock().stopAllProc(-1);
            retStatus = "OK";
        } else {
            // проверка типа формирования
            if (!Utl.in(tp, 0, 1, 2, 4)) {
                return "ERROR! Некорректный тип расчета: tp=" + tp;
            }

            String msg = null;
            // конфиг запроса
            House house = null;
            Vvod vvod = null;
            Ko ko = null;
            Org uk = null;
            if (reuId != null) {
                uk = orgDAO.getByReu(reuId);
                if (uk == null) {
                    retStatus = "ERROR! Задан некорректный reuId=" + reuId;
                    return retStatus;
                }
                msg = "УК reuId=" + reuId;
            } else if (houseId != 0) {
                house = em.find(House.class, houseId);
                if (house == null) {
                    retStatus = "ERROR! Задан некорректный houseId=" + houseId;
                    return retStatus;
                }
                msg = "дому houseId=" + houseId;
            } else if (vvodId != 0) {
                vvod = em.find(Vvod.class, vvodId);
                if (vvod == null) {
                    retStatus = "ERROR! Задан некорректный vvodId=" + vvodId;
                    return retStatus;
                }
                msg = "вводу vvodId=" + vvodId;
            } else if (klskId != 0) {
                ko = em.find(Ko.class, klskId);
                if (ko == null) {
                    retStatus = "ERROR! Задан некорректный klskId=" + klskId;
                    return retStatus;
                }
                msg = "помещению klskId=" + klskId;
            } else {
                if (Utl.in(tp, 0, 1)) {
                    msg = "всем помещениям";
                } else if (tp == 2) {
                    msg = "всем вводам";
                }
            }
            Usl usl = null;
            if (uslId != null) {
                assert msg != null;
                usl = em.find(Usl.class, uslId);
                if (usl == null) {
                    retStatus = "ERROR! Задан некорректный uslId=" + uslId;
                    return retStatus;
                }
            }

            Date genDt;
            try {
                genDt = genDtStr != null ? Utl.getDateFromStr(genDtStr) : null;
                retStatus = processMng.processWebRequest(tp, debugLvl, genDt, house, vvod, ko, uk, usl);
            } catch (ParseException e) {
                log.error(Utl.getStackTraceString(e));
                retStatus = "ERROR! Некорректная дата genDtStr=" + genDtStr;
            }
        }
        log.info("Статус: retStatus = {}", retStatus);
        return retStatus;
    }

    /**
     * Получить список элементов меню для итогового формирования
     */
    @RequestMapping(value = "/getSprgenitm", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<SprGenItm> getSprGenItm() {
        return sprGenItmDao.getAllOrdered();
    }

    /*
     * Вернуть статус текущего формирования
     * 0 - не формируется
     * 1 - идёт формирование
     */
    @RequestMapping(value = "/getStateGen", method = RequestMethod.GET)
    @ResponseBody
    public String getStateGen() {
        return config.getLock().isStopped(stopMarkAmntGen) ? "0" : "1";
    }

    /**
     * Получить последнюю ошибку
     */
    @RequestMapping(value = "/getPrepErr", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<PrepErr> getPrepErr() {
        return prepErrDao.getAllOrdered();
    }

    /**
     * Обновить элемент меню значениями
     *
     * @param lst - список объектов
     */
    @RequestMapping(value = "/editSprgenitm", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public void updateSprGenItm(@RequestBody List<SprGenItm> lst) { // использовать List объектов, со стороны ExtJs в
        // Модели сделано allowSingle: false
        execMng.updateSprGenItem(lst);
    }

    /*
     * Переключить состояние пунктов меню, в зависимости от формирования
     */
    @RequestMapping(value = "/checkItms", method = RequestMethod.POST)
    @ResponseBody
    public String checkItms(@RequestParam(value = "id") long id, @RequestParam(value = "sel") int sel) {
        execMng.execProc(35, id, sel);
        return null;
    }

    /*
     * Вернуть ошибку, последнего формирования, если есть
     */
    @RequestMapping(value = "/getErrGen", method = RequestMethod.GET)
    @ResponseBody
    public String getErrGen() {
        SprGenItm sprGenItm = sprGenItmDao.getByCd("GEN_ITG");

        return String.valueOf(sprGenItm.getErr());
    }


    /**
     * Начать итоговое формирование
     */
    @RequestMapping("/startGen")
    @ResponseBody
    public String startGen() {
        // почистить % выполнения
        execMng.clearPercent();
        // формировать
        GenMainMng genMainMng = ctx.getBean(GenMainMng.class);
        genMainMng.startMainThread();

        return "ok";
    }

/*
    @RequestMapping("/check")
    @ResponseBody
    public String check() {
        List<Kart> lstKart = penyaDAO.getKartWithDebitByGrpDeb(1);
        for (Kart kart : lstKart) {
            log.info("lsk={}", kart.getLsk());
        }
        return "ok";
    }

*/

    /**
     * Остановить итоговое формирование
     */
    @RequestMapping("/stopGen")
    @ResponseBody
    public String stopGen() {
        // установить статус - остановить формирование
        config.getLock().unlockProc(1, stopMarkAmntGen);
        config.getLock().unlockProc(1, stopMark);
        config.incProgress();
        return "ok";
    }

    /**
     * Остановить приложение
     */
    @RequestMapping("/terminateApp")
    public void terminateApp() {
        log.info("ВНИМАНИЕ! ЗАПРОШЕНА ОСТАНОВКА ПРИЛОЖЕНИЯ! - БЫЛ СОЗДАН ФАЙЛ c:\\Progs\\BillDirect\\stop");
        SpringApplication.exit(ctx, () -> 0);
    }

    /*
     * Вернуть прогресс текущего формирования, для обновления грида у клиента
     */
    @RequestMapping(value = "/getProgress", method = RequestMethod.GET)
    @ResponseBody
    public Integer getProgress() {
        return config.getProgress();
    }

    @RequestMapping("/migrate")
    public String migrate(
            @RequestParam(value = "lskFrom", defaultValue = "0") String lskFrom,
            @RequestParam(value = "lskTo", defaultValue = "0") String lskTo,
            @RequestParam(value = "debugLvl", defaultValue = "0") Integer debugLvl,
            @RequestParam(value = "key", defaultValue = "", required = false) String key) {
        log.info("GOT /migrate with: lskFrom={}, lskTo={}, debugLvl={}",
                lskFrom, lskTo, debugLvl);

        // проверка валидности ключа
        boolean isValidKey = checkValidKey(key);
        if (!isValidKey) {
            log.info("ERROR wrong key!");
            return "ERROR wrong key!";
        }
        // уровень отладки
        Integer dbgLvl = 0;
        if (debugLvl != null) {
            dbgLvl = debugLvl;
        }

        try {
            migrateMng.migrateAll(lskFrom, lskTo, dbgLvl);
        } catch (ErrorWhileGen errorWhileGen) {
            log.error(Utl.getStackTraceString(errorWhileGen));
            return "ERROR";
        }

        return "OK";
    }

    private boolean checkValidKey(String key) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH24mm");
        String str = "lasso_the_moose_" + formatter.format(new Date());
        log.info("Compare password with {}", str);
        return key.equals(str);
    }

    @RequestMapping("/checkCache")
    @ResponseBody
    public String checkCache() throws ParseException {
        log.info("check1={}",
                naborMng.getCached("bla1", null, Utl.getDateFromStr("01.01.2019")));
        log.info("check2={}",
                naborMng.getCached("bla1", null, null));
        log.info("check3={}",
                naborMng.getCached("bla1", null, Utl.getDateFromStr("01.01.2019")));
        log.info("check3={}",
                naborMng.getCached("bla1", null, Utl.getDateFromStr("01.01.2019")));
        log.info("check3={}",
                naborMng.getCached(null, null, null));
        log.info("");
        return "cached";
    }

    @RequestMapping("/checkCache2")
    @ResponseBody
    public void checkCache2() {
        Nabor nabor = em.find(Nabor.class, 41);
        log.info("check id=41 nabor.getId()={}, nabor.getOrg().getId()={}", nabor.getId(), nabor.getOrg().getId());
        nabor = em.find(Nabor.class, 42);
        log.info("check id=42 nabor.getId()={}, nabor.getOrg().getId()={}", nabor.getId(), nabor.getOrg().getId());
    }


    /**
     * Выполнение очистки L2 кэша Hibernate, содержащего сущности
     * Вызывать из Direct, из триггеров обновления справочников в Oracle
     */
    @RequestMapping("/evictL2C")
    @ResponseBody
    @Transactional
    public String evictL2C() {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        sessionFactory.getCache().evictEntityData();
        sessionFactory.getCache().evictCollectionData();
        log.info("ВНИМАНИЕ! Hbernate L2 Кэш очищен!");
        return "OK";
    }

    /**
     * Перезагрузить сущность Params
     */
    @RequestMapping("/reloadParams")
    @ResponseBody
    @Transactional
    public String reloadParams() {
        Param param = em.find(Param.class, 1);
        em.refresh(param);
        log.info("refreshed period={}", config.getPeriod());
        log.info("ВНИМАНИЕ! Сущность Params перезагружена!");
        return "OK";
    }


    /**
     * Загрузить файл внешних лиц.сч. во временную таблицу
     *
     * @param fileName - имя файла
     */
    @RequestMapping(value = "/loadFileKartExt/{fileName}", method = RequestMethod.GET)
    @ResponseBody
    public String loadFileKartExt(@PathVariable String fileName) {
        log.info("GOT /loadFileKartExt/ with {}", fileName);
        if (config.getLock().setLockProc(1, "loadFileKartExt")) {
            int cntLoaded = 0;
            try {
                List<Org> lstOrg = orgDAO.findByIsExchangeExt(true);
                for (Org org : lstOrg) {
                    cntLoaded = registryMng.loadFileKartExt(org,
                            "c:\\temp\\" + fileName);
                    break;// note делается пока по 1 орг
                }
            } catch (Exception e) {
                config.getLock().unlockProc(1, "loadFileKartExt");
                log.error(Utl.getStackTraceString(e));
                return "ERROR";
            }
            config.getLock().unlockProc(1, "loadFileKartExt");
            return String.valueOf(cntLoaded);
        } else {
            return "PROCESS";
        }
    }

    /**
     * Выгрузить файл платежей по внешнним лиц.сч.
     *
     * @param ordNum - порядковый номер файла за день
     */
    @RequestMapping(value = "/unloadPaymentFileKartExt/{ordNum}/{strDt1}/{strDt2}", method = RequestMethod.GET)
    @ResponseBody
    public String unloadPaymentFileKartExt(@PathVariable String ordNum,
                                           @PathVariable String strDt1, @PathVariable String strDt2) {
        log.info("GOT /unloadPaymentFileKartExt/ with {}", ordNum);
        String fileName = "";
        if (config.getLock().setLockProc(1, "unloadPaymentFileKartExt")) {
            Date genDt1;
            Date genDt2;
            try {
                genDt1 = strDt1 != null ? Utl.getDateFromStr(strDt1) : null;
                genDt2 = strDt2 != null ? Utl.getDateFromStr(strDt2) : null;
            } catch (ParseException e) {
                log.error(Utl.getStackTraceString(e));
                return "ERROR";
            }
            int cntLoaded = 0;
            try {
                Org rkc = orgDAO.getByOrgTp("РКЦ");
                List<Org> lstOrg = orgDAO.findByIsExchangeExt(true);
                for (Org org : lstOrg) {
                    fileName = "c:\\temp\\" + rkc.getInn() + "_" + Utl.getStrFromDate(new Date(), "yyyyMMdd")
                            + "_" + ordNum + ".txt";
                    cntLoaded = registryMng.unloadPaymentFileKartExt(fileName, org.getReu(), genDt1, genDt2);
                    break;
                }
            } catch (Exception e) {
                config.getLock().unlockProc(1, "unloadPaymentFileKartExt");
                log.error(Utl.getStackTraceString(e));
                return "ERROR";
            }
            config.getLock().unlockProc(1, "unloadPaymentFileKartExt");
            // вернуть кол-во выгружено, путь и имя файла
            return cntLoaded + "_" + fileName;
        } else {
            return "PROCESS";
        }
    }

    /**
     * Загрузить файл показаний по счетчикам
     *
     * @param fileName      - имя файла
     * @param isSetPrevious - установить предыдущее показание? (1-да, 0-нет) ВНИМАНИЕ! Текущие введёные показания будут сброшены назад
     */
    @RequestMapping(value = "/loadFileMeterVal/{fileName}/{isSetPrevious}", method = RequestMethod.GET)
    @ResponseBody
    public String loadFileMeterVal(@PathVariable String fileName, @PathVariable String isSetPrevious) {
        log.info("GOT /loadFileMeterVal/{}/{}", fileName, isSetPrevious);
        if (config.getLock().setLockProc(1, "loadFileMeterVal")) {
            int cntLoaded;
            try {
                boolean isSetPrev = false;
                if (isSetPrevious != null && isSetPrevious.equals("1")) {
                    isSetPrev = true;
                }
                log.info("fileName={}", fileName);
                cntLoaded = registryMng.loadFileMeterVal(
                        "c:\\temp\\" + fileName, "windows-1251",
                        isSetPrev
                );
            } catch (Exception e) {
                config.getLock().unlockProc(1, "loadFileMeterVal");
                log.error(Utl.getStackTraceString(e));
                return "ERROR";
            }
            config.getLock().unlockProc(1, "loadFileMeterVal");
            return String.valueOf(cntLoaded);
        } else {
            return "PROCESS";
        }
    }

    /**
     * Выгрузить файл показаний по счетчикам
     *
     * @param fileName - имя файла
     */
    @RequestMapping(value = "/unloadFileMeterVal/{fileName}/{strUk}", method = RequestMethod.GET)
    @ResponseBody
    public String unloadFileMeterVal(@PathVariable String fileName, @PathVariable String strUk) {
        log.info("GOT /unloadFileMeterVal/{}/{}", fileName, strUk);
        if (config.getLock().setLockProc(1, "unloadFileMeterVal")) {
            int cntLoaded = 1;
            try {
                cntLoaded = registryMng.unloadFileMeterVal(
                        "c:\\temp\\" + fileName, "windows-1251", strUk
                );
            } catch (Exception e) {
                config.getLock().unlockProc(1, "unloadFileMeterVal");
                log.error(Utl.getStackTraceString(e));
                return "ERROR";
            }
            config.getLock().unlockProc(1, "unloadFileMeterVal");
            return String.valueOf(cntLoaded);
        } else {
            return "PROCESS";
        }
    }

    @RequestMapping(value = "/loadApprovedKartExt", method = RequestMethod.GET)
    @ResponseBody
    public String loadApprovedKartExt() throws WrongParam {
        List<Org> lstOrg = orgDAO.findByIsExchangeExt(true);
        for (Org org : lstOrg) {
            registryMng.loadApprovedKartExt(org);
            break;// note делается пока по 1 орг
        }

        return "OK";
    }

    /**
     * Выполнение сжатия по всем таблицам и всем периодам, кроме текущего
     *
     * @param tableName имя таблицы, если "all", то взять все
     * @param key       - ключ для выполнения
     */
    @RequestMapping("/fullCompress")
    public String fullCompress(
            @RequestParam(value = "tableName") String tableName,
            @RequestParam(value = "key", defaultValue = "", required = false) String key,
            @RequestParam(value = "firstLsk") String firstLsk) {
        log.info("GOT /compress with: tableName={}, firstLsk={}", tableName, firstLsk);
        // проверка валидности ключа
        boolean isValidKey = checkValidKey(key);
        if (!isValidKey) {
            log.info("ERROR wrong key!");
            return "ERROR wrong key!";
        }

        if (tableName.equals("all")) {
            // все таблицы по очереди
            if (!mntBase.comprAllTables(firstLsk, null, "acharge", true)) {
                log.error("ОШИБКА! При сжатии таблицы {}!", tableName);
                // выйти при ошибке
                return "ERROR";
            }
            if (!mntBase.comprAllTables(firstLsk, null, "anabor", true)) {
                log.error("ОШИБКА! При сжатии таблицы {}!", tableName);
                // выйти при ошибке
                return "ERROR";
            }
            if (!mntBase.comprAllTables(firstLsk, null, "achargeprep", true)) {
                log.error("ОШИБКА! При сжатии таблицы {}!", tableName);
                // выйти при ошибке
                return "ERROR";
            }
            if (!mntBase.comprAllTables(firstLsk, null, "akartpr", true)) {
                log.error("ОШИБКА! При сжатии таблицы {}!", tableName);
                // выйти при ошибке
                return "ERROR";
            }
            if (!mntBase.comprAllTables(firstLsk, null, "chargepay", true)) {
                log.error("ОШИБКА! При сжатии таблицы {}!", tableName);
                // выйти при ошибке
                return "ERROR";
            }
        } else {
            if (!mntBase.comprAllTables(firstLsk, null, tableName, true)) {
                log.error("ОШИБКА! При сжатии таблицы {}!", tableName);
                // выйти при ошибке
                return "ERROR";
            }
        }
        return "OK";
    }

    /*
     * Получить короткие наименования действующих услуг по лиц.счету
     */
    @RequestMapping(value = "/getKartShortNames/{lsk}/{period}", method = RequestMethod.GET)
    @ResponseBody
    @Transactional
    public String getKartShortNames(
            @PathVariable String lsk,
            @PathVariable String period
    ) {
        log.info("GOT /getKartShortNames with: lsk={}, period={}", lsk, period);
        Kart kart = em.find(Kart.class, lsk);
        return kartMng.getUslNameShort(kart, 0, 5, ",",
                period, config.getPeriod());
    }


}