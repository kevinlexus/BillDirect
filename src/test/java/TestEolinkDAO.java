import com.dic.app.Config;
import com.dic.bill.dao.EolinkDAO2;
import com.dic.bill.dto.HouseUkTaskRec;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.model.scott.Kart;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@EnableCaching
@DataJpaTest
@Slf4j
public class TestEolinkDAO {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private EolinkDAO2 eolinkDAO2;
	@Autowired
	private EolinkMng eolinkMng;

	@Test
	@Transactional(propagation = Propagation.REQUIRED)
	@Rollback(true)
	public void testKo() {
		Kart kart = em.find(Kart.class, "00000170");
		log.info("{}={}", kart.getLsk(), kart.getKoKw());
	}

	@Test
	@Transactional(propagation = Propagation.REQUIRED)
	@Rollback(true)
	public void testGetKartNotExistsInEolink() {
		log.info("Test eolinkDAO2.GetKartNotExistsInEolink - Start");

		List<Kart> lst = eolinkMng.getKartNotExistsInEolink(707492, 707491);
		for (Kart kart : lst) {
			log.info("Отсутствующий в EOLINK лиц счет в квартирах с подъездом:{}", kart.getLsk());
		}
		log.info("Test eolinkDAO2.GetKartNotExistsInEolink - End");
	}

	@Test
	@Transactional(propagation = Propagation.REQUIRED)
	@Rollback(true)
    public void testGetHouseByTpWoTaskTp() {
		log.info("Test eolinkDAO.getHouseByTpWoTaskTp - Start");

		for (HouseUkTaskRec t : eolinkDAO2.getHouseByTpWoTaskTp("GIS_EXP_HOUSE", "GIS_EXP_ACCS")) {
			log.info("check house.id={}, task.id={}, uk.id={}", t.getEolHouseId(), t.getMasterTaskId(), t.getEolUkId());
		}

/*
		Eolink eolink = em.find(Eolink.class, 706814);
		Eolink procUk = em.find(Eolink.class, 708111);
		Task parent = em.find(Task.class, 1544869);
		Task master = em.find(Task.class, 1544862);
		Lst2 lst2 = em.find(Lst2.class, 145);
		Task task = Task.builder()
				.withEolink(eolink)
				.withParent(parent)
				.withMaster(master)
				.withState("STP")
				.withAct(lst2)
				.withFk_user(1)
				.withErrAckCnt(0)
				.withProcUk(procUk)
				.withTrace(0).build();
		em.persist(task); // note Используй crud.save
		eolinkDAO2.getHouseByTpWoTaskTp(null, null);
*/
	}


}
