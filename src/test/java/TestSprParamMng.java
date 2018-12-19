import com.dic.app.Config;
import com.dic.bill.mm.SprParamMng;
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
import java.util.Date;

import static junit.framework.TestCase.assertTrue;

/**
 * Тестирование сервиса SprParam
 * @author lev
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestSprParamMng {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private SprParamMng sprParamMng;

	@Test
    public void testPdoc() throws Exception {
		log.info("Start");
		Double n1 = sprParamMng.getN1("GEN_SAL_PEN");
		log.info("GEN_SAL_PEN={}", n1);
		assertTrue(n1 != null);

		String s1 = sprParamMng.getS1("FIRST_MONTH");
		log.info("FIRST_MONTH={}", s1);
		assertTrue(s1 != null);

		Date d1 = sprParamMng.getD1("MONTH_HEAT3");
		log.info("MONTH_HEAT3={}", d1);
		assertTrue(d1 != null);
		log.info("End");
    }



}
