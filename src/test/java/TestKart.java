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
import com.ric.cmn.excp.WrongGetMethod;
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
import org.springframework.util.StopWatch;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import java.math.BigDecimal;

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
				3, true, true, true, 1);

		// выполнить расчет
		genChrgProcessMng.genChrg(calcStore, ko, reqConf);

	}

	/**
	 * Проверка корректности расчета начисления по дому
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void genChrgProcessMngGenChrgHouse() throws WrongParam, ErrorWhileChrg, ErrorWhileChrgPen, WrongGetMethod {
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
		// Х.в.
		testDataBuilder.addVvodForTest(house, "011", 1, false,
				new BigDecimal("110.279"), true);

		// Г.в.
		testDataBuilder.addVvodForTest(house, "015", 1, false,
				new BigDecimal("82.23"), true);

		// Отопление Гкал
		testDataBuilder.addVvodForTest(house, "053", 1, false,
				new BigDecimal("500.2568"), false);

		// Х.В. для ГВС
		testDataBuilder.addVvodForTest(house, "099", 1, false,
				new BigDecimal("40.23"), true);

		// Тепловая энергия для нагрева Х.В.
		testDataBuilder.addVvodForTest(house, "103", 6, // тип 6 не распределяется по лиц.счетам
				false,
				new BigDecimal("7.536"), true);

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

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);

		StopWatch sw = new org.springframework.util.StopWatch();
		sw.start("TIMING:Распределение объемов");
		// ВЫЗОВ распределения объемов
		for (Vvod vvod : house.getVvod()) {
			BigDecimal amntVolChrg = BigDecimal.ZERO;
			BigDecimal amntVolChrgPrep = BigDecimal.ZERO;
			reqConf.setVvod(vvod);
			distVolMng.distVolByVvod(reqConf, calcStore);

/*
			for (Nabor nabor : vvod.getNabor()) {
				if (nabor.getUsl().getId().equals("011")) {
					for (Charge t : nabor.getKart().getCharge()) {
						log.info("ОДН:Charge lsk={}, usl={}, type={}, testOpl={}", t.getKart().getLsk(), t.getUsl().getId(),
								t.getType(), t.getTestOpl());
						amntVolChrg = amntVolChrg.add(t.getTestOpl());
					}
				}
			}
			for (Nabor nabor : vvod.getNabor()) {
				if (nabor.getUsl().getId().equals("011")) {
					for (ChargePrep t : nabor.getKart().getChargePrep()) {
						log.info("ОДН:ChargePrep lsk={}, usl={}, sch={}, tp={}, vol={}",
								t.getKart().getLsk(), t.getUsl().getId(), t.isExistMeter(),
								t.getTp(), t.getVol());
						amntVolChrgPrep = amntVolChrgPrep.add(t.getVol());
					}
				}
			}
			log.info("Итоговое распределение ОДН charge={}, chargePrep={}", amntVolChrg, amntVolChrgPrep);
*/
		}
		sw.stop();
		// получить объемы
		for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
			if (t.kart.getLsk().equals("РСО_0001") && Utl.in(t.usl.getId(),"099")) {
				log.info("CHECK1 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}

		reqConf.setVvod(null);
		reqConf.setHouse(house);
		reqConf.setTp(0);

		// загрузить справочники todo еще раз??????????
		calcStore = processMng.buildCalcStore(reqConf.getGenDt(), 0);

		sw.start("TIMING:Начисление");
		// вызов начисления
		processMng.genProcessAll(reqConf, calcStore);
		sw.stop();
		log.info("");
		// получить объемы
/*
		for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
			if (Utl.in(t.usl.getId(),"011")) {
				log.info("CHECK2 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}
		log.info("");
*/
/*
		for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
			if (t.kart.getLsk().equals("РСО_0001") && Utl.in(t.usl.getId(),"015")) {
				log.info("CHECK2 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}
*/
		log.info("");
		for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
			if (t.kart.getLsk().equals("РСО_0001") && Utl.in(t.usl.getId(),"099")) {
				log.info("CHECK2 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}
/*
		log.info("");
		for (UslVolKart t : calcStore.getChrgCountAmount().getLstUslVolKart()) {
			if (t.kart.getLsk().equals("РСО_0001") && Utl.in(t.usl.getId(),"015")) {
				log.info("CHECK3 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}
*/
		log.info("");
		for (UslVolKart t : calcStore.getChrgCountAmount().getLstUslVolKart()) {
			if (t.kart.getLsk().equals("РСО_0001") && Utl.in(t.usl.getId(),"103")) {
				log.info("CHECK3 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}
/*
		log.info("");
		for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
			if (Utl.in(t.usl.getId(),"103")) {
				log.info("CHECK2 lsk={}, usl={} vol={} ar={} Kpr={}",
						t.kart.getLsk(), t.usl.getId(),
						t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
						t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
			}
		}
*/
		System.out.println(sw.prettyPrint());
		log.info("Test genChrgProcessMngGenChrgHouse End!");
	}
}