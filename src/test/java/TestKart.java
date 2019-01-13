import com.dic.app.Config;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistVolMng;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dto.*;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileChrgPen;
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
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestKart {

	@Autowired
	private TestDataBuilder testDataBuilder;
	@Autowired
	private KartMng kartMng;
	@Autowired
	private ProcessMng processMng;
	@Autowired
	private GenChrgProcessMng genChrgProcessMng;
	@Autowired
	private DistVolMng distVolMng;
	@Autowired
	private ConfigApp config;

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
	 * Проверка корректности расчета начисления по квартире
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void genChrgProcessMngGenChrgAppartment() throws WrongParam, ErrorWhileChrg {
		log.info("Test genChrgProcessMngGenChrgAppartment");

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);

		// дом
		House house = em.find(House.class, 6091);

		// построить лицевые счета по квартире
		Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(63.52),
				3, true, true, true, 1);

		// выполнить расчет
		genChrgProcessMng.genChrg(calcStore, ko);

	}

	/**
	 * Проверка корректности расчета начисления по дому
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void genChrgProcessMngGenChrgHouse() throws WrongParam, ErrorWhileChrg, ErrorWhileChrgPen {
		log.info("Test genChrgProcessMngGenChrgHouse Start!");
		// конфиг запроса
		RequestConfig reqConf =
				RequestConfig.RequestConfigBuilder.aRequestConfig()
						.withRqn(config.incNextReqNum()) // уникальный номер запроса
						.withTp(2) // тип операции - распределение объема
						.build();

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);
		// дом
		House house = new House();
		Ko houseKo = new Ko();

		house.setKo(houseKo);
		house.setKul("0001");
		house.setNd("000001");

		// добавить вводы
		// Отопление Гкал
		testDataBuilder.addVvodForTest(house, "053", 1, true);

		// построить лицевые счета по квартире
		testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(63.52),
				3,true, true, true, 1);
		testDataBuilder.buildKartForTest(house, "0002", BigDecimal.valueOf(50.24),
				2,true, true, true, 1);
		testDataBuilder.buildKartForTest(house, "0003", BigDecimal.valueOf(75.89),
				2,true, true, true,1);
		// нежилое
		testDataBuilder.buildKartForTest(house, "0004", BigDecimal.valueOf(22.01),
				1,true, true, true, 9);
		testDataBuilder.buildKartForTest(house, "0005", BigDecimal.valueOf(67.1),
				4,true, true, true,1);

		// ВЫЗОВ распределения
		for (Vvod vvod : house.getVvod()) {
			reqConf.setVvod(vvod);
			distVolMng.distVolByVvod(reqConf);
		}

		log.info("Test genChrgProcessMngGenChrgHouse End!");

	}
}
