package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dao.PrepErrDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
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
    private ProcessMng debitMng;
    @Autowired
    private MigrateMng migrateMng;
    @Autowired
    private ExecMng execMng;
    @Autowired
    private SprGenItmDAO sprGenItmDao;
    @Autowired
    private PrepErrDAO prepErrDao;
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
     * @param houseId   - houseId объекта (дом)
     * @param vvodId   - vvodId объекта (ввод)
     * @param klskId   - klskId объекта (помещение)
     * @param debugLvl - уровень отладки 0, null - не записивать в лог отладочную информацию, 1 - записывать
     * @param genDtStr   - дата на которую сформировать
     * @param stop     - 1 - остановить выполнение текущей операции с типом tp
     * @param key      - ключ, для выполнения ответственных заданий
     */
    @RequestMapping("/gen")
    public String gen(
            @RequestParam(value = "tp", defaultValue = "0") int tp,
            @RequestParam(value = "houseId", defaultValue = "0", required = false) int houseId,
            @RequestParam(value = "vvodId", defaultValue = "0", required = false) int vvodId,
            @RequestParam(value = "klskId", defaultValue = "0", required = false) int klskId,
            @RequestParam(value = "debugLvl", defaultValue = "0") int debugLvl,
            @RequestParam(value = "genDt", defaultValue = "", required = false) String genDtStr,
            @RequestParam(value = "key", defaultValue = "", required = false) String key,
            @RequestParam(value = "stop", defaultValue = "0", required = false) int stop
    ) {
        log.info("GOT /gen with: tp={}, key={}, debugLvl={}, genDt={}, houseId={}, vvodId={}, klskId={}, stop={}",
                tp, key, debugLvl, genDtStr, houseId, vvodId, klskId, stop);

        // проверка типа формирования
        if (!Utl.in(tp, 0, 1, 2)) {
            return "ERROR! Некорректный тип расчета: tp=" + tp;
        }

        // конфиг запроса
        House house = null;
        Vvod vvod = null;
        Ko ko = null;
        if (houseId != 0) {
            house = em.find(House.class, houseId);
        } else if (vvodId != 0) {
            vvod = em.find(Vvod.class, vvodId);
        } else if (klskId != 0) {
            ko = em.find(Ko.class, klskId);
        } else {
            return "ERROR! Незаполнен объект расчета - houseId, vvodId, klskId";
        }
        RequestConfig reqConf =
                RequestConfig.RequestConfigBuilder.aRequestConfig()
                        .withTp(tp)
                        .withGenDt(genDtStr != null ? Utl.getDateFromStr(genDtStr) : null)
                        .withHouse(house)
                        .withVvod(vvod)
                        .withKo(ko)
                        .withCurDt1(config.getCurDt1())
                        .withCurDt2(config.getCurDt2())
                        .withDebugLvl(debugLvl)
                        .withRqn(config.incNextReqNum())
                        .build();

        if (stop == 1) {
            // Остановка длительного процесса
            config.getLock().stopProc(reqConf.getRqn(), stopMark);
        } else {
            // проверить переданные параметры
            String err = reqConf.checkArguments();
            if (err == null) {

                if (Utl.in(reqConf.getTp(), 0, 1)) {
                    // загрузить хранилище
                    CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);
                    // расчет начисления, задолженности и пени
                    try {
                        processMng.genProcessAll(reqConf, calcStore);
                    } catch (ErrorWhileGen errorWhileGen) {
                        errorWhileGen.printStackTrace();
                        return "ERROR! Ошибка в процессе расчета";
                    }
                } else if (reqConf.getTp() == 2) {
                    // распределение объемов
                    try {
                        processMng.distVol(reqConf);
                    } catch (ErrorWhileGen errorWhileGen) {
                        errorWhileGen.printStackTrace();
                        return "ERROR! Ошибка при распределении объемов";
                    }
                }
            } else {
                return err;
            }
        }
        return "OK";
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
    public String checkItms(@RequestParam(value = "id") int id, @RequestParam(value = "sel") int sel) {
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
}