import com.dic.app.Config;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import java.util.List;

import static junit.framework.TestCase.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestKart {

	@Autowired
	private KartMng kartMng;
	@Autowired
	private ProcessMng processMng;
	@Autowired
	private GenChrgProcessMng genChrgProcessMng;
	@Autowired
	private StatesPrDAO statesPrDao;
	@PersistenceContext
	private EntityManager em;

	/**
	 * Тест запроса на поиск k_lsk_id квартиры по параметрам
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void isWorkKartMngGetKlskByKulNdKw() throws Exception {

		log.info("-----------------Begin");
		Ko ko = kartMng.getKoByKulNdKw("0174", "000012", "0000066");
		log.info("Получен klsk={}", ko.getId());
		Assert.assertTrue(ko.getId().equals(105392));

		log.info("-----------------End");
	}

	/**
	 * Проверка корректности расчета начисления
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void checkChrg() {
		log.info("Test StatePr count");

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);
		// построить лиц.счет
		Kart kart = kartMng.buildKartForTest("0000000X");
		em.persist(kart);
		// выполнить расчет
		genChrgProcessMng.genChrg(calcStore, kart);

	}


	/**
	 * Проверка корректности получения статусов DAO уровнем
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void checkStatesPrDaoGetByDate() {
		log.info("Test checkStatesPrDaoGetByDate");

		// создание сущностей
		Kart kart = kartMng.buildKartForTest("0000000X");
		// проживающие
		KartPr kartPr = kartMng.addKartPrForTest(kart, 1, 3, "Иванов", "01.01.1973",
				"01.04.2014", "20.04.2014");

		kartMng.addStatusPrForTest(kartPr, 1, "01.02.2014", "15.04.2014");
		kartMng.addStatusPrForTest(kartPr, 2, "10.04.2014", "13.04.2014");
		kartMng.addStatusPrForTest(kartPr, 3, "13.04.2014", "20.04.2014");
		kartMng.addStatusPrForTest(kartPr, 4, "21.04.2014", "27.04.2014");

		em.persist(kart);

		// запрос
		List<StatePr> statePr = statesPrDao.getByDate(kartPr.getId(),Utl.getDateFromStr("05.03.2014"));
		assertTrue(statePr.get(0).getStatusPr().getId().equals(1));

		statePr = statesPrDao.getByDate(kartPr.getId(),Utl.getDateFromStr("12.04.2014"));
		for (StatePr pr : statePr) {
			log.info("**** Id={}", pr.getStatusPr().getId());
			if (pr.getStatusPr().getTp().getCd().equals("PROP")) {
				assertTrue(pr.getStatusPr().getId().equals(1));
			} else {
				assertTrue(pr.getStatusPr().getId().equals(2));
			}
		}

		statePr = statesPrDao.getByDate(kartPr.getId(),Utl.getDateFromStr("22.04.2014"));
		assertTrue(statePr.get(0).getStatusPr().getId().equals(4));

		statePr = statesPrDao.getByDate(kartPr.getId(),Utl.getDateFromStr("17.04.2014"));
		assertTrue(statePr.get(0).getStatusPr().getId().equals(3));

		// статус с открытыми датами
		kartPr.getStatePr().clear(); // очистить статусы

		kartMng.addStatusPrForTest(kartPr, 4, null, null);

		statePr = statesPrDao.getByDate(kartPr.getId(),Utl.getDateFromStr("22.04.2030"));
		log.info("status={}", statePr.get(0).getStatusPr().getId());
		assertTrue(statePr.get(0).getStatusPr().getId().equals(4));

		log.info("Test end");
	}



}
