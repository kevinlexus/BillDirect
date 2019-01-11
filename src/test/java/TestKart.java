import com.dic.app.Config;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.dto.*;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
		Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(63.52), 3, true, true, true);

		// выполнить расчет
		genChrgProcessMng.genChrg(calcStore, ko.getId());

	}

	/**
	 * Проверка корректности расчета начисления по дому
	 */
	@Test
	@Rollback(true)
	@Transactional
	public void genChrgProcessMngGenChrgHouse() throws WrongParam, ErrorWhileChrg {
		log.info("Test genChrgProcessMngGenChrgHouse Start!");

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);
		// дом
		House house = new House();
		Ko houseKo = new Ko();

		house.setKo(houseKo);
		house.setKul("0001");
		house.setNd("000001");
		/*em.persist(houseKo);
		em.persist(house);
*/
		// построить лицевые счета по квартире
		testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(63.52), 3,true, true, true);
		testDataBuilder.buildKartForTest(house, "0002", BigDecimal.valueOf(50.24), 2,true, true, true);
		testDataBuilder.buildKartForTest(house, "0003", BigDecimal.valueOf(75.89), 2,true, true, true);
		testDataBuilder.buildKartForTest(house, "0004", BigDecimal.valueOf(22.01), 1,true, true, true);
		testDataBuilder.buildKartForTest(house, "0005", BigDecimal.valueOf(67.1), 4,true, true, true);

		// получить distinct klsk помещений, выполнить расчет
		for (Integer t : house.getKart().stream()
				.map(t->t.getKoKw().getId()).distinct().collect(Collectors.toList())) {
			genChrgProcessMng.genChrg(calcStore, t);
			//log.info("***************** ={}", calcStore.getChrgCountHouse().getLstUslPriceVol().size());
		}

		// объемы по дому:
		log.info("Объемы по дому:");
		for (ChrgCount d : calcStore.getChrgCountHouse().getLstChrgCount()) {
			for (UslPriceVol t : d.getLstUslPriceVol()) {
				if (Utl.in(t.usl.getId(),"003")) {
					log.info("lsk={} usl={} cnt={} " +
									"empt={} " +
									"vol={} volOvSc={} volEmpt={} area={} areaOvSc={} " +
									"areaEmpt={} kpr={} kprOt={} kprWr={}",
							t.kart.getLsk(),
							t.usl.getId(), t.isCounter, t.isEmpty,
							t.vol.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.volOverSoc.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.volEmpty.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.area.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.areaOverSoc.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.areaEmpty.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.kpr.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.kprOt.setScale(5, BigDecimal.ROUND_HALF_UP),
							t.kprWr.setScale(5, BigDecimal.ROUND_HALF_UP));
				}
			}
		}

		log.info("Кол-во квартир по дому:");

		Stream<Usl> streamUsl = calcStore.getChrgCountHouse().getLstChrgCount().stream()
				.flatMap(t -> t.getLstUslPriceVol().stream()).map(t -> t.usl).distinct();
		streamUsl.forEach(s-> {
			long cntKo = calcStore.getChrgCountHouse().getLstChrgCount().stream()
					.filter(t -> t.getLstUslPriceVol().stream().anyMatch(d -> d.usl.equals(s)))
					.distinct().count();
			log.info("usl={}, cntKo={}", s.getId(), cntKo);
		});

		log.info("Test genChrgProcessMngGenChrgHouse End!");

	}
}
