import com.dic.app.Config;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
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

import java.math.BigDecimal;
import java.util.Iterator;
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
	public void genChrgProcessMngGenChrg() throws WrongParam {
		log.info("Test genChrgProcessMngGenChrg");

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);
		// построить лиц.счет
		Kart kart = kartMng.buildKartForTest("0000000X", true, true, true);

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
		Kart kart = kartMng.buildKartForTest("0000000X", false, false, true);
		// проживающие
		KartPr kartPr = kartMng.addKartPrForTest(kart, 1, 3, "Антонов", "01.01.1973",
				"01.04.2014", "20.04.2014");

		kartMng.addStatePrForTest(kartPr, 4, "01.02.2014", "01.03.2014");
		kartMng.addStatePrForTest(kartPr, 1, "02.03.2014", "09.04.2014");
		kartMng.addStatePrForTest(kartPr, 2, "10.04.2014", "13.04.2014");
		kartMng.addStatePrForTest(kartPr, 3, "14.04.2014", "20.04.2014");
		kartMng.addStatePrForTest(kartPr, 4, "21.04.2014", "27.04.2014");

		em.persist(kart);

		// запрос
		//List<StatePr> statePr = statesPrDao.findByDate(kart.getLsk(), Utl.getDateFromStr("01.04.2014"),
		//		Utl.getDateFromStr("30.04.2014"));
		List<StatePr> statePr = statesPrDao.findByDate(kart.getLsk(), Utl.getDateFromStr("01.04.2014"),
				Utl.getDateFromStr("30.04.2014"));
		log.info("Выборка:");
		int i=0;
		for (StatePr pr : statePr) {
			i++;
			log.info("fio={}, dt1={}, dt2={}, statusId={}",
					pr.getKartPr().getFio(), pr.getDtFrom(), pr.getDtTo(), pr.getStatusPr().getId());
			if (Utl.between(Utl.getDateFromStr("10.04.2014"),
					pr.getDtFrom(), pr.getDtTo()) && pr.getKartPr().getFio().equals("Антонов")) {
				assertTrue(pr.getStatusPr().getId().equals(2));
			}
			if (Utl.between(Utl.getDateFromStr("15.04.2014"),
					pr.getDtFrom(), pr.getDtTo()) && pr.getKartPr().getFio().equals("Антонов")) {
				assertTrue(pr.getStatusPr().getId().equals(3));
			}
			if (Utl.between(Utl.getDateFromStr("22.04.2014"),
					pr.getDtFrom(), pr.getDtTo()) && pr.getKartPr().getFio().equals("Антонов")) {
				assertTrue(pr.getStatusPr().getId().equals(4));
			}
		}
		assertTrue(i==4);

		i=0;
		// обработать пустой период
		kartPr.getStatePr().clear();
		kartMng.addStatePrForTest(kartPr, 1, null, "11.04.2015");
		statePr = statesPrDao.findByDate(kart.getLsk(), Utl.getDateFromStr("01.04.2014"),
				Utl.getDateFromStr("30.04.2014"));

		log.info("Выборка: пустой период");
		for (StatePr pr : statePr) {
			i++;
			log.info("fio={}, dt1={}, dt2={}, statusId={}",
					pr.getKartPr().getFio(), pr.getDtFrom(), pr.getDtTo(), pr.getStatusPr().getId());
			if (Utl.between(Utl.getDateFromStr("10.04.2014"),
					pr.getDtFrom(), pr.getDtTo()) && pr.getKartPr().getFio().equals("Антонов")) {
				assertTrue(pr.getStatusPr().getId().equals(1));
			}
		}
		assertTrue(i==1);

		// обработать полностью пустой период
		kartPr.getStatePr().clear();
		kartMng.addStatePrForTest(kartPr, 2, null, null);
		statePr = statesPrDao.findByDate(kart.getLsk(), Utl.getDateFromStr("01.04.2014"),
				Utl.getDateFromStr("30.04.2014"));

		log.info("Выборка: полностью пустой период");
		for (StatePr pr : statePr) {
			i++;
			log.info("fio={}, dt1={}, dt2={}, statusId={}",
					pr.getKartPr().getFio(), pr.getDtFrom(), pr.getDtTo(), pr.getStatusPr().getId());
			if (Utl.between(Utl.getDateFromStr("30.05.2014"),
					pr.getDtFrom(), pr.getDtTo()) && pr.getKartPr().getFio().equals("Антонов")) {
				assertTrue(pr.getStatusPr().getId().equals(2));
			}
		}
		assertTrue(i==2);

		log.info("Test end");
	}



}
