import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dic.app.AppConfig;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.VchangeDetDAO;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=AppConfig.class)
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

	@Test
	public void mainWork() {
		log.info("Test Start!");

		log.info("Test Debit");
		debUslDao.getDebitByLsk("00000084", 201403).forEach(t-> {
			log.info("Debit: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getDt(), t.getTp());
		});

		log.info("Test Charge");
		chargeDao.getChargeByLsk("00000084").forEach(t-> {
			log.info("Charge: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getDt(), t.getTp());
		});

		log.info("Test VchangeDet");
		vchangeDetDao.getVchangeDetByLsk("00000084").forEach(t-> {
			log.info("Change: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getDt(), t.getTp());
		});

		log.info("Test KwtpDay оплата долга");
		kwtpDayDao.getKwtpDaySumByLsk("00000084").forEach(t-> {
			log.info("KwtpDay tp=1: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getDt(), t.getTp());
		});

		log.info("Test KwtpDay оплата пени");
		kwtpDayDao.getKwtpDayPenByLsk("00000084").forEach(t-> {
			log.info("KwtpDay tp=0: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getDt(), t.getTp());
		});

		log.info("Test CorrectPay");
		correctPayDao.getCorrectPayByLsk("00000084").forEach(t-> {
			log.info("CorrectPay: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getDt(), t.getTp());
		});

		log.info("Test End!");
	}


}
