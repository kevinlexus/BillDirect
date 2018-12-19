import com.dic.app.Config;
import com.dic.bill.dao.KwtpDAO;
import com.dic.bill.model.scott.Kwtp;
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
public class TestKwtp {

	@PersistenceContext
    private EntityManager em;

	@Autowired
	private KwtpDAO kwtpDao;

	@Test
    public void testPdoc() throws Exception {
		log.info("Start");
		Kwtp kwtp = kwtpDao.getByNumDoc("13213416454169849645645988555558");
		log.info("Kwtp.id={}", kwtp.getId());
		log.info("End");
    }


}
