import com.dic.app.Config;
import com.dic.bill.dao.AchargeDAO;
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
public class TestAchargeDAO {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private AchargeDAO achargeDao;

	/**
	 * Проверить, работают ли запросы
	 * @throws Exception
	 */
	@Test
	@Rollback(false)
    public void isWorkAFlowDAOgetChrgGrp() throws Exception {

		log.info("-----------------Begin");
        achargeDao.getChrgGrp("00000049", 201404, 706813).forEach(t -> {
            log.info("Check Ulist.id={}, chrg={}, chng={}, vol={}, price={}",
                    t.getUlistId(), t.getChrg(), t.getChng(), t.getVol(), t.getPrice());
        });
        log.info("-----------------End");
    }


}
