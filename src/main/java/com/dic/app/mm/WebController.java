package com.dic.app.mm;

import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dic.bill.RequestConfig;
import com.dic.bill.mm.Config;
import com.dic.bill.model.scott.SessionDirect;
import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class WebController {

	@Autowired
	private DebitMng debitMng;
	@Autowired
	private Config config;
	@PersistenceContext
    private EntityManager em;

	/**
	 * Расчет
	 * @param tp - тип расчета (0- долги и пеня)
	 * @param lsk - лиц.счет, если отсутствует - весь фонд
	 * @param sessionId - Id сессии, устанавливается в UTILS.prep_users_tree,
	 * 					  если не заполнен, использовать 0, т.е. без записи в отчетную таблицу
	 * @param debugLvl - уровень отладки 0, null - не записивать в лог отладочную информацию, 1 - записывать
	 * @genDt - дата формирования
	 * @return
	 */
	@RequestMapping("/gen")
    public String gen (
    		@RequestParam(value = "tp", defaultValue = "0") String tp,
    		@RequestParam(value = "lsk", defaultValue = "0") String lsk,
    		@RequestParam(value = "sessionId", defaultValue = "0")  Integer sessionId,
    		@RequestParam(value = "debugLvl", defaultValue = "0") Integer debugLvl,
    		@RequestParam(value = "genDt", defaultValue = "", required = false) String genDt1
    		) {
		log.info("GOT /gen with: tp={}, lsk={}, sessionId={}, debugLvl={}, genDt={}", tp, lsk, sessionId, debugLvl, genDt1);

		if (tp == null || !Utl.in(tp, "0")) {
			return "ERROR1 некорректный тип расчета: tp="+tp;
		}
		Date genDt = checkDate(genDt1);
		if (genDt == null) {
			return "ERROR2 некорректная дата расчета: genDt="+genDt1;
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

		// способ формирования
		String lskPar;
		if (lsk.equals("0")) {
			lskPar = null;
		} else {
			lskPar = lsk;
		}
		try {
	    	debitMng.genDebitAll(lskPar, genDt, dbgLvl, reqConf);
		} catch (Exception e) {
			return "ERROR " + e.getMessage();
		}

        return "OK";
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