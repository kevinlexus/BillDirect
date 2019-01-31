import com.dic.app.Config;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.model.scott.Kart;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Тестирование DAO Kart
 * @author lev
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestKartDAO {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private KartDAO kartDao;

	/**
	 * Тест запроса на поиск помещения по параметрам
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
    public void isWorkKartDAOFindByKulNdKw() throws Exception {

		log.info("-----------------Begin");
		int i=0;
		List<Kart> lstKart;
		lstKart = kartDao.findByKulNdKw("0174", "000012", "0000066");
		// проверить совпадение id дома
		for (Kart kart : lstKart) {
			log.info("Проверка лиц.счета="+kart.getLsk());
			assertTrue(kart.getLsk().equals("14042300"));
			assertTrue(kart.getHouse().getId().equals(37985));
			i++;
		}
		assertTrue(i>0);
        log.info("-----------------End");
    }


}
