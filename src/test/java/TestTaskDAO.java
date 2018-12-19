import com.dic.app.Config;
import com.dic.bill.dao.TaskDAO;
import com.dic.bill.model.exs.Task;
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
 * Тестирование DAO уровня сущности Task
 * @author lev
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestTaskDAO {

	@PersistenceContext
    private EntityManager em;

	@Autowired
	private TaskDAO taskDao;

	@Test
    public void testPdoc() throws Exception {
		log.info("Start");
		Task task = em.find(Task.class, 1544216);
		log.info("Start method");
/*		taskDao.getByTaskAddrTp(task, "ЛС", null, 1).stream()
				.filter(t-> t.getAct().getCd().equals("GIS_ADD_ACC")).collect(Collectors.toList()).forEach(t-> {
						log.info("id={}", t.getId());
				});
*/

		log.info("End");
    }



}
