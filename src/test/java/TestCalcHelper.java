import com.dic.app.Config;
import com.dic.bill.dao.NotifDAO;
import com.dic.bill.dao.PdocDAO;
import com.dic.bill.helper.CalcHelper;
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
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;

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
public class TestCalcHelper {

	@PersistenceContext
    private EntityManager em;

	@Autowired
	private CalcHelper calcHelper;



}
