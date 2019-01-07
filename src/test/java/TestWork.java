import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.app.mm.ProcessMng;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.Repeat;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dic.app.Config;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.RedirPayDAO;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

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
	private TestDataBuilder testDataBuilder;
	@Autowired
	private ProcessMng processMng;

	@PersistenceContext
    private EntityManager em;

	/**
	 * Проверка расчета задолжности и пени
	 * @throws ErrorWhileChrgPen
	 */
    @Test
    @Rollback(true)
    @Repeat(value = 1)
	@Transactional
    public void mainWork() throws ErrorWhileChrgPen {
		log.info("Test mainWork");

		log.info("Текущий период: dt1={}, dt2={}", config.getCurDt1(), config.getCurDt2());
		// конфиг запроса
		RequestConfig reqConf =
				RequestConfig.RequestConfigBuilder.aRequestConfig()
				.withRqn(config.incNextReqNum()) // уникальный номер запроса
				.withTp(1) // тип операции
				.build();

		processMng.genProcessAll("00000008", "00000008",
				Utl.getDateFromStr("15.04.2014"),
				0, reqConf);

	}


}
