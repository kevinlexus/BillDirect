package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dao.PrepErrDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.PrepErr;
import com.dic.bill.model.scott.SprGenItm;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistDeb;
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
public class WebController {

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
    private ConfigApp config;
    @Autowired
    private ApplicationContext ctx;


    /**
     * Расчет
     *
     * @param tp        - тип выполнения 0-начисление, 1-задолженность и пеня, 2 - распределение объемов по вводу
     * @param klskId   - klskId объекта (дом, ввод, помещение)
     * @param lskFrom   - начальный лиц.счет, если отсутствует - весь фонд
     * @param lskTo     - конечный лиц.счет, если отсутствует - весь фонд
     * @param debugLvl  - уровень отладки 0, null - не записивать в лог отладочную информацию, 1 - записывать
     * @param genDt1 - дата на которую сформировать
     * @param stop - 1 - остановить выполнение текущей операции с типом tp
     * @param key - ключ, для выполнения ответственных заданий
     */
    @RequestMapping("/gen")
    public String gen (
            @RequestParam(value = "tp", defaultValue = "0") int tp,
            @RequestParam(value = "klskId", defaultValue = "0", required = false) long klskId,
            @RequestParam(value = "lskFrom", defaultValue = "0", required = false) String lskFrom,
            @RequestParam(value = "lskTo", defaultValue = "0", required = false) String lskTo,
            @RequestParam(value = "debugLvl", defaultValue = "0") int dbgLvl,
            @RequestParam(value = "genDt", defaultValue = "", required = false) String genDt1,
            @RequestParam(value = "key", defaultValue = "", required = false) String key,
            @RequestParam(value = "stop", defaultValue = "0", required = false) int stop
    ) {
        log.info("GOT /gen with: tp={}, lskFrom={}, lskTo={}, debugLvl={}, genDt={}, stop={}",
                tp, lskFrom, lskTo, dbgLvl, genDt1, stop);

        // проверка валидности ключа
        boolean isValidKey = checkValidKey(key);
        if (!isValidKey) {
            log.info("ERROR wrong key!");
            return "ERROR wrong key!";
        }
        // проверка типа формирования
        if (!Utl.in(tp, 0, 1, 2)) {
            return "ERROR1 некорректный тип расчета: tp="+tp;
        }

        Date genDt = null;
        // остановить процесс?
        boolean isStopped = false;
        if (stop == 1) {
            isStopped = true;
        } else {
            // если не принудительная остановка, то значит - выполнение, проверить параметры
            if (tp == 1) {
                // задолженность и пеня, - проверить текущую дату
                genDt = checkDate(genDt1);
                if (genDt == null) {
                    return "ERROR2 некорректная дата расчета: genDt="+genDt1;
                }
            }
        }

        // конфиг запроса
        RequestConfig reqConf =
                RequestConfig.RequestConfigBuilder.aRequestConfig()
                        .withTp(tp)
                        .withRqn(config.incNextReqNum()) // уникальный номер запроса
                        .build();

        if (tp == 1) {
            // рассчитать долги и пеню
            try {
                if (!isStopped) {
                    // если не остановка процесса
                    if (!lskFrom.equals(lskTo)) {
                        // заблокировать при расчете по всем лиц.счетам
                        Boolean isLocked = config.getLock().setLockProc(reqConf.getRqn(),
                                "ProcessGeneration");
                        if (isLocked) {
                            try {
                                debitMng.genProcessAll(reqConf, genDt, dbgLvl);
                            } finally {
                                // разблокировать при расчете по всем лиц.счетам
                                config.getLock().unlockProc(reqConf.getRqn(),
                                        "ProcessGeneration");
                            }
                        } else {
                            return "ERROR ОШИБКА блокировки процесса расчета задолженности и пени";
                        }
                    } else {
                        // по одному лиц.счету
                        debitMng.genProcessAll(reqConf, genDt, dbgLvl);
                    }
                } else {
                    // снять маркер выполнения процесса
                    config.getLock().stopProc(reqConf.getRqn(),
                            "ProcessGeneration");
                }
            } catch (Exception e) {
                log.error(Utl.getStackTraceString(e));
                return "ERROR " + e.getMessage();
            }

            return "OK";
        } else {
            return "ERROR Не найден вызываемый метод";
        }
    }

    /**
     * Получить список элементов меню для итогового формирования
     *
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
     *
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
     *
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
     *
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


    /**
     * Проверить дату формирования
     *
     * @param dt - дата в виде строки
     * @return - дата Date, если null - невалидная входящая дата
     */
    private Date checkDate(String dt) {
        // проверка на заполненные даты, если указаны
        if (dt == null || dt.length() == 0) {
            return null;
        }

        Date genDt = Utl.getDateFromStr(dt);

        // проверить, что дата в диапазоне текущего периода
        if (!Utl.between(genDt, config.getCurDt1(), config.getCurDt2())) {
            return null;
        }

        return genDt;
    }

}