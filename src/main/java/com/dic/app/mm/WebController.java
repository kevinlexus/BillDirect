package com.dic.app.mm;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.dic.bill.RequestConfig;
import com.dic.bill.dao.PrepErrDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.PrepErr;
import com.dic.bill.model.scott.SessionDirect;
import com.dic.bill.model.scott.SprGenItm;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistDeb;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class WebController {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private DebitMng debitMng;
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
	 * Получить список элементов меню для итогового формирования
	 * @return
	 */
    @RequestMapping(value = "/getSprgenitm", method = RequestMethod.GET, produces="application/json")
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
    	return config.getLock().isStopped("MainGeneration") ? "0":"1";
    }

	/**
	 * Получить последнюю ошибку
	 * @return
	 */
	@RequestMapping(value = "/getPrepErr", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public List<PrepErr> getPrepErr() {
		return prepErrDao.getAllOrdered();
	}

	/**
	 * Обновить элемент меню значениями
	 * @param iList
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
	 * Начать формирование
	 * @return
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
	 * Остановить формирование
	 * @return
	 */
	@RequestMapping("/stopGen")
    @ResponseBody
    public String stopGen() {
		// установить статус - остановить формирование
		config.getLock().unlockProc(1, "MainGeneration");
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


	/**
	 * Расчет
	 * @param tp - тип расчета (0- долги и пеня)
	 * @param lskFrom - начальный лиц.счет, если отсутствует - весь фонд
	 * @param lskTo - конечный лиц.счет, если отсутствует - весь фонд
	 * @param sessionId - Id сессии, устанавливается в UTILS.prep_users_tree,
	 * 					  если не заполнен, использовать 0, т.е. без записи в отчетную таблицу
	 * @param debugLvl - уровень отладки 0, null - не записивать в лог отладочную информацию, 1 - записывать
	 * @key - ключ, для выполнения ответственных заданий
	 * @genDt - дата формирования
	 * @return
	 */
	@RequestMapping("/gen")
    public String gen (
    		@RequestParam(value = "tp", defaultValue = "0") String tp,
    		@RequestParam(value = "lskFrom", defaultValue = "0") String lskFrom,
    		@RequestParam(value = "lskTo", defaultValue = "0") String lskTo,
    		@RequestParam(value = "sessionId", defaultValue = "0")  Integer sessionId,
    		@RequestParam(value = "debugLvl", defaultValue = "0") Integer debugLvl,
    		@RequestParam(value = "genDt", defaultValue = "", required = false) String genDt1,
    		@RequestParam(value = "key", defaultValue = "", required = false) String key,
    		@RequestParam(value = "stop", defaultValue = "0", required = false) String stop
    		) {
		log.info("GOT /gen with: tp={}, lskFrom={}, lskTo={}, sessionId={}, debugLvl={}, genDt={}, stop={}",
				tp, lskFrom, lskTo, sessionId, debugLvl, genDt1, stop);

		// проверка валидности ключа
		boolean isValidKey = checkValidKey(key);
		if (!isValidKey) {
			log.info("ERROR wrong key!");
			return "ERROR wrong key!";
		}
		// проверка типа формирования
		if (tp == null || !Utl.in(tp, "0")) {
			return "ERROR1 некорректный тип расчета: tp="+tp;
		}
		Date genDt = null;
		// остановить процесс?
		boolean isStopped = false;
		if (stop.equals("1")) {
			isStopped = true;
		} else {
			// если не принудительная остановка, проверить параметры
			genDt = checkDate(genDt1);
			if (genDt == null) {
				return "ERROR2 некорректная дата расчета: genDt="+genDt1;
			}
		}

		SessionDirect sessionDirect = null;
		if (sessionId != 0) {
			// получить сессию Директа
			sessionDirect = em.find(SessionDirect.class, sessionId);
			if (sessionDirect == null) {
				return "ERROR3 не найдена сессия Директ с Id=" + sessionId;
			}
		}

		// конфиг запроса
		RequestConfig reqConf =
				RequestConfig.builder()
				.withRqn(config.incNextReqNum()) // уникальный номер запроса
				.withSessionDirect(sessionDirect) // сессия Директ
				.build();

		// уровень отладки
		Integer dbgLvl = 0;
		if (debugLvl != null) {
			dbgLvl = Integer.valueOf(debugLvl);
		}

		if (tp.equals("0")) {
			// рассчитать долги и пеню
			try {
				if (!isStopped) {
					// если не остановка процесса
					if (!lskFrom.equals(lskTo)) {
						// заблокировать при расчете по всем лиц.счетам
						Boolean isLocked = config.getLock().setLockProc(reqConf.getRqn(), "MainGeneration"); // TODO разрулить потом маркеры - будут пересекаться с формированием из SprGenItem!
						if (isLocked) {
							try {
								debitMng.genDebitAll(lskFrom, lskTo, genDt, dbgLvl, reqConf);
							} finally {
								// разблокировать при расчете по всем лиц.счетам
								config.getLock().unlockProc(reqConf.getRqn(), "MainGeneration"); // TODO разрулить потом маркеры - будут пересекаться с формированием из SprGenItem!
							}
						} else {
							return "ERROR ОШИБКА блокировки процесса расчета задолженности и пени";
						}
					} else {
						// по одному лиц.счету
				    	debitMng.genDebitAll(lskFrom, lskTo, genDt, dbgLvl, reqConf);
					}
				} else {
					// снять маркер выполнения процесса
					config.getLock().stopProc(reqConf.getRqn(), "MainGeneration"); // TODO разрулить потом маркеры - будут пересекаться с формированием из SprGenItem!
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



	@RequestMapping("/migrate")
    public String migrate (
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
			dbgLvl = Integer.valueOf(debugLvl);
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
		boolean checkKey = key.equals("lasso_the_moose_".concat(str));
		return checkKey;
	}


	/**
	 * Проверить дату формирования
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
		if (!Utl.between(genDt, config.getCurDt1(), config.getCurDt2()))  {
			return null;
		}

		return genDt;
	}

}