import com.dic.app.Config;
import com.dic.bill.dao.NotifDAO;
import com.dic.bill.dao.PdocDAO;
import com.dic.bill.mm.PdocMng;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Исправный модуль, для тестирования Spring beans
 * @author lev
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestPdoc {

	@PersistenceContext
    private EntityManager em;

	@Autowired
	private PdocDAO pdocDao;
	@Autowired
	private NotifDAO notifDao;
	@Autowired
	private PdocMng pdocMng;

	@Test
    public void testPdoc() throws Exception {
		log.info("Start");

/*
		BigDecimal bd1 = new BigDecimal("500");
		BigDecimal bd2 = BigDecimal.valueOf(100);
		log.info("summa={}", bd1.divide(bd2));
*/
/*		pdocDao.getForLoadByHouseWithEntry(7570).stream().forEach(t-> {
			log.info("Платежный документ для лс с подъездом, Pdoc.id={}, Pdoc.cd={}", t.getId(), t.getCd());
		}); ;

		pdocDao.getForLoadByHouseWOEntry(7570).stream().forEach(t-> {
			log.info("Платежный документ для лс без подъезда, Pdoc.id={}, Pdoc.cd={}", t.getId(), t.getCd());
		}); ;

		Eolink houseEol = em.find(Eolink.class, 7570);
		pdocMng.getPdocForLoadByHouse(houseEol).stream().forEach(t-> {
			log.info("Платежный документ Pdoc.id={}, Pdoc.cd={}", t.getId(), t.getCd());
		}); ;
*/
		//BigDecimal sal = Utl.nvl(debMng.getDebAmnt("62020006", null, "201805", 0), BigDecimal.ZERO);
		//log.info("ПД: sal={}", sal);

		notifDao.getForLoadByHouseWithEntry(7570).forEach(t-> {
			log.info("Платежный документ Notif.id={}", t.getId());
		});

		log.info("End");
    }


}
