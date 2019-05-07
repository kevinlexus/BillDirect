import com.dic.app.Config;
import com.dic.app.mm.CorrectsMng;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dto.MeterData;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Meter;
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
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
		log.info("Test correctsMng.corrPayByCreditSalExceptSomeUsl");

		// дом
		House house = new House();
		Ko houseKo = new Ko();
		em.persist(houseKo);

		house.setKo(houseKo);
		house.setKul("0001");
		house.setNd("000001");
		em.persist(house);

		// построить лицевые счета по помещению
		int ukId = 12; // УК 14,15
		Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(76.2),
				3, true, true, 1, 1, ukId);
		em.persist(ko);
		String lsk = "ОСН_0001";
		Kart kart = em.find(Kart.class, lsk);

		// Добавить сальдо
		testDataBuilder.buildSaldoUslForTest(kart, "003", 7, "201404", "10.62");
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

		correctsMng.corrPayByCreditSalExceptSomeUsl();

		log.info("-----------------End");
	}


}