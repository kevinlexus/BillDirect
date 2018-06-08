import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dic.app.Config;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DebitMng;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.RedirPayDAO;
import com.dic.bill.model.scott.SessionDirect;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestWork {


	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private ConfigApp config;
	@Autowired
	private ThreadMng threadMng;
	@Autowired
	private DebitMng debitMng;

	@Autowired
	private KartDAO kartDao;

	@Autowired
	private DebDAO debPenUslDao;
	@Autowired
	private RedirPayDAO redirPayDao;
	@PersistenceContext
    private EntityManager em;

    @Test
    @Rollback(false)
    //@Repeat(value = 100)
    public void mainWork() throws ErrorWhileChrgPen {
		log.info("Test start");


		log.info("Текущий период: dt1={}, dt2={}", config.getCurDt1(), config.getCurDt2());

		SessionDirect sessionDirect = em.find(SessionDirect.class, 4735);
		// конфиг запроса
		RequestConfig reqConf =
				RequestConfig.builder()
				.withRqn(config.incNextReqNum()) // уникальный номер запроса
				.withSessionDirect(sessionDirect) // сессия Директ
				.build();

		Boolean isLocked = config.getLock().setLockProc(reqConf.getRqn(), "debitMng.genDebitAll");
		//debitMng.genDebitAll("00000185", "00000185", Utl.getDateFromStr("15.04.2014"), 0, reqConf);
		debitMng.genDebitAll("00000185", "90000185", Utl.getDateFromStr("15.04.2014"), 0, reqConf);

		/*
		if (isLocked) {
			debitMng.genDebitAll(null, Utl.getDateFromStr("15.04.2014"), 0, reqConf);
		}*/

		log.info("Test end");
	}


}
