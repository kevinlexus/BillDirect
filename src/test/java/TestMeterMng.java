import com.dic.app.Config;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dto.MeterData;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.exs.Eolink;
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
import java.text.ParseException;
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
public class TestMeterMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private MeterDAO meterDao;
	@Autowired
	private MeterMng meterMng;

	/**
	 * Тест на наличие показаний счетчика по GUID и Timestamp
	 *
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void isFindMeterDataByGuidTs() throws Exception {
		class LocalMeterData implements MeterData {
			public Date getTs() {
				try {
					return Utl.getDateFromStr("30.03.2014");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return null;
			}

			public String getGuid() {
				return "2312-1316-4564-4654-4463";
			}
		}

		int i = 0, i2 = 0;
		log.info("-----------------Begin");
		// найти счетчики х.в. по объекту Ko
		XMLGregorianCalendar ts = Utl.getXMLDate(Utl.getDateFromStr("30.03.2014"));
		List<MeterData> lst = new ArrayList<>(5);
		MeterData elem = new LocalMeterData();
		lst.add(elem);
		lst.add(elem);
		lst.add(elem);
		lst.add(elem);

		assertTrue(meterMng.getIsMeterDataExist(lst, "2312-1316-4564-4654-4463", ts));

		log.info("-----------------End");
	}

	/**
	 * Проверка OneToOne
	 *
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void justTest() throws Exception {
		Ko ko = em.find(Ko.class, 524877);
		Eolink eolink = ko.getEolink();
		log.info("Eolink={}", eolink.getId());

		Eolink e2 = em.find(Eolink.class, 707565);
		Ko ko2 = e2.getKoObj();
		log.info("Ko={}", ko2.getId());

		Meter meter = em.find(Meter.class, 73522);
		Ko ko3 = meter.getKo();
		Ko ko4 = meter.getKoObj();
		log.info("Ko3={}, Ko4={}", ko3.getId(), ko4.getId());

		Kart kart = em.find(Kart.class, "00000239");

		log.info("Ko5={}, Ko6={}", kart.getKoKw().getId(), kart.getKoLsk().getId());
	}

	/**
	 * Проверка MeterMng.getIsMeterActual
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void isWorkingGetIsMeterActual() throws Exception
	{
		Eolink meterEol = em.find(Eolink.class, 708094);
		Ko ko = meterEol.getKoObj();
		Meter meter = ko.getMeter();
		log.info("actual dt1={}, dt2={}", meter.getDt1(), meter.getDt2());
	}


}