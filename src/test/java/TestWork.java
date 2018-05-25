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

import com.dic.app.AppConfig;
import com.dic.app.mm.DebitMng;
import com.dic.bill.Config;
import com.dic.bill.dao.KartDAO;
import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=AppConfig.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestWork {


	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private Config config;
	@Autowired
	private DebitMng debitMng;

	@Autowired
	private KartDAO kartDao;

    @Test
    @Rollback(false)
    public void mainWork() {
		log.info("Test start");

		log.info("Текущий период: dt1={}, dt2={}", config.getCurDt1(), config.getCurDt2());

		//log.info("days={}", Utl.daysBetween(Utl.getDateFromStr("22.05.2018"), Utl.getDateFromStr("23.05.2018")));

		//debitMng.genDebitAll("00000084", Utl.getDateFromStr("15.04.2014"), 0);

		kartDao.getAll().forEach(t-> {
			log.info("lsk={}", t.getLsk());
			debitMng.genDebitAll(t.getLsk(), Utl.getDateFromStr("15.04.2014"), 0);
		});

		log.info("Test end");
	}


}
