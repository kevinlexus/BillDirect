package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dao.OrgDAO;
import com.dic.bill.dao.PrepErrDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.mm.NaborMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.*;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@RestController
@Slf4j
public class WebController implements CommonConstants {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private NaborMng naborMng;
    @Autowired
    private MigrateMng migrateMng;
    @Autowired
    private ExecMng execMng;
    @Autowired
    private SprGenItmDAO sprGenItmDao;
    @Autowired
    private PrepErrDAO prepErrDao;
    @Autowired
    private OrgDAO orgDAO;
    @Autowired
    private ProcessMng processMng;
    @Autowired
    private ConfigApp config;
    @Autowired
    private ApplicationContext ctx;


    /**
     * Расчет
     *
     * @param tp       - тип выполнения 0-начисление, 1-задолженность и пеня, 2 - распределение объемов по вводу
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
            @RequestParam(value = "genDt", defaultValue = "", required = false) String genDtStr,
            @RequestParam(value = "stop", defaultValue = "0", required = false) int stop
    ) {
        log.info("GOT /gen with: tp={}, debugLvl={}, genDt={}, reuId={}, houseId={}, vvodId={}, klskId={}, uslId={}, stop={}",
                tp, debugLvl, genDtStr, reuId, houseId, vvodId, klskId, uslId, stop);
        String retStatus = "ERROR";
        if (stop == 1) {
            // Остановка всех процессов (отмена формирования например)
            config.getLock().stopAllProc(-1);
            retStatus = "OK";
        } else {
            // проверка типа формирования
            if (!Utl.in(tp, 0, 1, 2)) {
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
                if (uk==null) {
                    retStatus = "ERROR! Задан некорректный reuId=" + reuId;
                    return retStatus;
                }
                msg = "УК reuId=" + reuId;
            } else if (houseId != 0) {
                house = em.find(House.class, houseId);
                if (house==null) {
                    retStatus = "ERROR! Задан некорректный houseId=" + houseId;
                    return retStatus;
                }
                msg = "дому houseId=" + houseId;
            } else if (vvodId != 0) {
                vvod = em.find(Vvod.class, vvodId);
                if (vvod==null) {
                    retStatus = "ERROR! Задан некорректный vvodId=" + vvodId;
                    return retStatus;
                }
                msg = "вводу vvodId=" + vvodId;
            } else if (klskId != 0) {
                ko = em.find(Ko.class, klskId);
                if (ko==null) {
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
                if (usl==null) {
                    retStatus = "ERROR! Задан некорректный uslId=" + uslId;
                    return retStatus;
                }
                msg = msg.concat(", по услуге uslId=" + uslId);
            }
            Date genDt = genDtStr != null ? Utl.getDateFromStr(genDtStr) : null;
            // построить запрос
            RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                    .withTp(tp)
                    .withGenDt(genDt)
                    .withUk(uk)
                    .withHouse(house)
                    .withVvod(vvod)
                    .withKo(ko)
                    .withUsl(usl)
                    .withCurDt1(config.getCurDt1())
                    .withCurDt2(config.getCurDt2())
                    .withDebugLvl(debugLvl)
                    .withRqn(config.incNextReqNum())
                    .withIsMultiThreads(true)
                    .build();
            reqConf.prepareId();
            StopWatch sw = new org.springframework.util.StopWatch();
            sw.start("TIMING: " + reqConf.getTpName());

            log.info("");
            log.info("Задано: {} по {}", reqConf.getTpName(), msg);
            log.info("");
            // проверить переданные параметры
            retStatus = reqConf.checkArguments();
            if (retStatus == null) {
                try {
                    if (Utl.in(reqConf.getTp(), 0, 1, 2)) {
                        // расчет начисления, распределение объемов, расчет задолженности и пени
                        reqConf.prepareChrgCountAmount();
                        log.info("Будет обработано {} объектов", reqConf.getLstItems().size());
                        processMng.genProcessAll(reqConf);
                    }
                    retStatus = "OK";
                } catch (Exception e) {
                    retStatus = "ERROR! Ошибка при выполнении расчета!";
                    log.error(Utl.getStackTraceString(e));
                } finally {
                    config.getLock().unlockProc(reqConf.getRqn(), stopMark);
                }
            }
            sw.stop();
            System.out.println(sw.prettyPrint());
            log.info("");
            log.info("Выполнено: {} по {}", reqConf.getTpName(), msg);
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
        return config.getLock().isStopped("AmountGeneration") ? "0" : "1";
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

    /**
     * Остановить итоговое формирование
     */
    @RequestMapping("/stopGen")
    @ResponseBody
    public String stopGen() {
        // установить статус - остановить формирование
        config.getLock().unlockProc(1, "AmountGeneration");
        return "ok";
    }

    /**
     * Остановить приложение
     */
    @RequestMapping("/terminateApp")
    public void terminateApp() {
        log.info("ВНИМАНИЕ! ЗАПРОШЕНА ОСТАНОВКА ПРИЛОЖЕНИЯ!");
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
        } catch (ErrorWhileDistDeb e) {
            log.error("Прошла ошибка, в процессе миграции данных по задолженностям");
            log.error(Utl.getStackTraceString(e));
        }

        return "OK";

    }

    private boolean checkValidKey(String key) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String str = formatter.format(new Date());
        return key.equals("lasso_the_moose_".concat(str));
    }

    @RequestMapping("/checkCache")
    @ResponseBody
    public String checkCache() {
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

    /**
     * Выполнение очистки кэша, связанного с Nabor, Price, Kart
     *
     * @return
     */
    @RequestMapping("/evictCacheNaborKartPrice")
    @ResponseBody
    @CacheEvict(value = {"KartMng.getKartMainLsk",
            "PriceMng.multiplyPrice",
            "ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    public String evictCacheNaborKartPrice() {
        log.info("Кэш 'NaborMng.getDetailUslPrice' очищен!");
        return "OK";
    }
}