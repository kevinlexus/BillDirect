package com.dic.app.mm;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dic.bill.model.scott.SessionDirect;
import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class WebController {

	@Autowired
	private DebitMng debitMng;
	@PersistenceContext
    private EntityManager em;

	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет, если отсутствует - весь фонд
	 * @param sessionId - Id сессии, устанавливается в UTILS.prep_users_tree,
	 * 					  если не заполнен, использовать 0, т.е. без записи в отчетную таблицу
	 * @return
	 */
	@RequestMapping("/genDebitPen")
    public String genDebitPen(@RequestParam(value = "lsk", defaultValue = "0") String lsk,
    		@RequestParam(value = "sessionId", defaultValue = "0") Integer sessionId
    		) {
		log.info("GOT /genDebitPen with: lsk={}", lsk);

		SessionDirect sessionDirect = null;
		if (sessionId != 0) {
			// получить сессию Директа
			sessionDirect = em.find(SessionDirect.class, sessionId);
			if (sessionDirect == null) {
				return "ERROR не найдена сессия Директ с Id=" + sessionId;
			}
		}

		// способ формирования
		String lskPar;
		if (lsk.equals("0")) {
			lskPar = null;
		} else {
			lskPar = lsk;
		}
		try {
	    	debitMng.genDebitAll(lskPar, Utl.getDateFromStr("15.04.2014"), 0, sessionDirect);
		} catch (Exception e) {
			return "ERROR " + e.getMessage();
		}

        return "OK";
    }

}