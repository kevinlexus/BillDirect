import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dic.app.Config;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.VchangeDetDAO;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestDAO {


	@Autowired
	private DebDAO debUslDao;
	@Autowired
	private ChargeDAO chargeDao;
	@Autowired
	private VchangeDetDAO vchangeDetDao;
	@Autowired
	private KwtpDayDAO kwtpDayDao;
	@Autowired
	private CorrectPayDAO correctPayDao;
	@Autowired
	private SaldoUslDAO saldoUslDao;

	@Test
	public void mainWork() {
		log.info("Test Start!");
/*
		log.info("Test Debit");
		debUslDao.getDebitByLsk("00000084", 201403).forEach(t-> {
			log.info("Debit: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test Charge");
		chargeDao.getChargeByLsk("00000084").forEach(t-> {
			log.info("Charge: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test VchangeDet");
		vchangeDetDao.getVchangeDetByLsk("00000084").forEach(t-> {
			log.info("Change: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test KwtpDay оплата долга");
		kwtpDayDao.getKwtpDaySumByLsk("00000084").forEach(t-> {
			log.info("KwtpDay tp=1: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test KwtpDay оплата пени");
		kwtpDayDao.getKwtpDayPenByLsk("00000084").forEach(t-> {
			log.info("KwtpDay tp=0: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test CorrectPay");
		correctPayDao.getCorrectPayByLsk("00000084").forEach(t-> {
			log.info("CorrectPay: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});
*/
		log.info("Test VchargePay");
		saldoUslDao.getVchargePayByLsk("00000085", 201403).forEach(t-> {
			log.info("VchargePay: mg={}, summa={}", t.getMg(), t.getSumma());
		});

		log.info("Test ChargeNabor");
		saldoUslDao.getChargeNaborByLsk("00000085", 201403).forEach(t-> {
			log.info("ChargeNabor: uslId={}, orgId={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
		});

		log.info("Test SaldoUsl");
		saldoUslDao.getSaldoUslByLsk("00000085", "201403").forEach(t-> {
			log.info("SaldoUsl: uslId={}, orgId={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
		});
		log.info("Test End!");
	}


}
