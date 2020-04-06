import com.dic.app.Config;
import com.dic.app.mm.CorrectsMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;

import static org.junit.Assert.assertTrue;

/**
 * Тестирование сервиса MeterMng
 *
 * @author lev
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestCorrectsMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private CorrectsMng correctsMng;
	@Autowired
	private TestDataBuilder testDataBuilder;

	/**
	 * Тест корректировочной проводки
	 *
	 */
	@Test
	@Rollback(true)
	public void testCorrPayByCreditSalExceptSomeUsl() throws Exception {
		log.info("Test correctsMng.corrPayByCreditSal");

		// дом
		House house = new House();
		Ko houseKo = new Ko();
		em.persist(houseKo); // note Используй crud.save

		house.setKo(houseKo);
		house.setKul("0001");
		house.setNd("000001");
		em.persist(house); // note Используй crud.save

		// построить лицевые счета по помещению
		int ukId = 12; // УК 14,15
		Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(76.2),
				3, true, true, 1, 1, ukId);
		em.persist(ko); // note Используй crud.save
		String lsk = "ОСН_0001";
		Kart kart = em.find(Kart.class, lsk);

		// Добавить сальдо
		testDataBuilder.buildSaldoUslForTest(kart, "003", 7, "201404", "-10.62");
		testDataBuilder.buildSaldoUslForTest(kart, "003", 3, "201404", "105.78");
		testDataBuilder.buildSaldoUslForTest(kart, "005", 3, "201404", "552.17");
		testDataBuilder.buildSaldoUslForTest(kart, "004", 3, "201404", "22.83");
		testDataBuilder.buildSaldoUslForTest(kart, "004", 2, "201404", "14.77");
		testDataBuilder.buildSaldoUslForTest(kart, "004", 9, "201404", "-211.88");
		testDataBuilder.buildSaldoUslForTest(kart, "007", 1, "201404", "-14.25");
		testDataBuilder.buildSaldoUslForTest(kart, "005", 1, "201404", "-16.81");
		testDataBuilder.buildSaldoUslForTest(kart, "006", 1, "201404", "-18.96");
		testDataBuilder.buildSaldoUslForTest(kart, "006", 2, "201404", "-180.55");
		testDataBuilder.buildSaldoUslForTest(kart, "006", 3, "201404", "-158.99");

		correctsMng.corrPayByCreditSal(1, Utl.getDateFromStr("01.04.2014"), "'001','002','003'");

		log.info("-----------------End");
	}


}