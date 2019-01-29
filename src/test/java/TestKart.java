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
import com.ric.cmn.excp.*;
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
import org.springframework.util.StopWatch;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import java.math.BigDecimal;
import java.text.DecimalFormat;

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

		// конфиг запроса
		RequestConfig reqConf =
				RequestConfig.RequestConfigBuilder.aRequestConfig()
						.withRqn(config.incNextReqNum()) // уникальный номер запроса
						.withTp(0) // тип операции - начисление
						.build();

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);

		// дом
		House house = em.find(House.class, 6091);

		// построить лицевые счета по квартире
		Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(63.52),
				3, true, true, 1, 1);

		// выполнить расчет
		genChrgProcessMng.genChrg(calcStore, ko, reqConf);

	}

	/**
	 * Проверка корректности расчета начисления по дому
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void genChrgProcessMngGenChrgHouse() throws WrongParam, ErrorWhileChrg, ErrorWhileChrgPen, WrongGetMethod, ErrorWhileDist {
		log.info("Test genChrgProcessMngGenChrgHouse Start!");
		// конфиг запроса
		RequestConfig reqConf =
				RequestConfig.RequestConfigBuilder.aRequestConfig()
						.withRqn(config.incNextReqNum()) // уникальный номер запроса
						.withTp(2) // тип операции - распределение объема
						.withIsMultiThreads(false) // для Unit - теста однопоточно!
						.build();

		// дом
		House house = new House();
		Ko houseKo = new Ko();

		house.setKo(houseKo);
		house.setKul("0001");
		house.setNd("000001");

		// добавить вводы

		// без ОДПУ
		// Х.в.
		testDataBuilder.addVvodForTest(house, "011", 4, false,
				null, true);

		// Г.в.
		testDataBuilder.addVvodForTest(house, "015", 5, false,
				null, true);

/*
		// с ОДПУ
		// Х.в.
		testDataBuilder.addVvodForTest(house, "011", 1, false,
				new BigDecimal("150.2796"), true);

		// Г.в.
		testDataBuilder.addVvodForTest(house, "015", 1, false,
				new BigDecimal("162.23"), true);
*/

		// Отопление Гкал
		testDataBuilder.addVvodForTest(house, "053", 1, false,
				new BigDecimal("500.2568"), false);

		// Х.В. для ГВС
		testDataBuilder.addVvodForTest(house, "099", 1, false,
				new BigDecimal("140.23"), true);

		// Тепловая энергия для нагрева Х.В.
		testDataBuilder.addVvodForTest(house, "103", 6, // тип 6 не распределяется по лиц.счетам
				false,
				new BigDecimal("7.536"), true);

		// Эл.эн. ОДН (вариант с простым распределением по площади)
		testDataBuilder.addVvodForTest(house, "123", 1, false,
				new BigDecimal("120.58"), false);

		// построить лицевые счета по квартире
		testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(63.52),
				3,true, true, 1, 1);
		testDataBuilder.buildKartForTest(house, "0002", BigDecimal.valueOf(50.24),
				2,true, true, 1, 2);
		testDataBuilder.buildKartForTest(house, "0003", BigDecimal.valueOf(75.89),
				2,true, true,1, 3);
		// нежилое
		testDataBuilder.buildKartForTest(house, "0004", BigDecimal.valueOf(22.01),
				1,true, true, 9,1);
		testDataBuilder.buildKartForTest(house, "0005", BigDecimal.valueOf(67.1),
				4,true, true,1, 2);
		// нормативы по услугам х.в. г.в.
		testDataBuilder.buildKartForTest(house, "0006", BigDecimal.valueOf(35.12),
				2,true, false,1, 0);

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);

		StopWatch sw = new org.springframework.util.StopWatch();
		sw.start("TIMING:Распределение объемов");
		// ВЫЗОВ распределения объемов
		for (Vvod vvod : house.getVvod()) {
			reqConf.setVvod(vvod);
			distVolMng.distVolByVvod(reqConf, calcStore);
		}
		sw.stop();


		reqConf.setVvod(null);
		reqConf.setHouse(house);
		reqConf.setTp(0);

		// загрузить справочники todo еще раз??????????
		calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);

		sw.start("TIMING:Начисление");
		// вызов начисления
		processMng.genProcessAll(reqConf, calcStore);
		sw.stop();

		// распечатать объемы

		/*
		calcStore.getChrgCountAmount().printVolAmnt(null, "056");
		calcStore.getChrgCountAmount().printVolAmnt(null, "015");
		calcStore.getChrgCountAmount().printVolAmnt(null, "057");
		calcStore.getChrgCountAmount().printVolAmnt(null, "123");
*/

		calcStore.getChrgCountAmount().printVolAmnt(null, "099");
		calcStore.getChrgCountAmount().printVolAmnt(null, "101");

		/*
		calcStore.getChrgCountAmount().printVolAmnt(null, "053");
*/

		System.out.println(sw.prettyPrint());
		log.info("Test genChrgProcessMngGenChrgHouse End!");
	}

}
