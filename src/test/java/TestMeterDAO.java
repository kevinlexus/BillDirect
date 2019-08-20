import com.dic.app.Config;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dto.MeterData;
import com.dic.bill.dto.SumMeterVol;
import com.dic.bill.mm.MeterMng;
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
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Тестирование DAO Meter
 * @author lev
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestMeterDAO {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private MeterDAO meterDao;
	@Autowired
	private MeterMng meterMng;

	/**
	 * Тест запроса на поиск счетчика по коду услуги и объекту Ko
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
    public void isWorkKartDAOFindByKulNdKw() throws Exception {

/*
		int i=0, i2 = 0;
		log.info("-----------------Begin");
		// найти счетчики х.в. по объекту Ko
		Date dt = Utl.getDateFromStr("30.03.2014");
		List<Meter> meterLst = meterDao.findActualByKoUsl(105392, "011", dt);
		for (Meter meter : meterLst) {
			log.info("Х.В. счетчик: Meter.id={}", meter.getId());
			assertTrue(meter.getId().equals(72910) ||meter.getId().equals(73522));
			i++;
	}
		// найти счетчики г.в. по объекту Ko
		meterLst = meterDao.findActualByKoUsl(105392, "015", dt);
		for (Meter meter : meterLst) {
			log.info("Г.В. счетчик: Meter.id={}", meter.getId());
			assertTrue(meter.getId().equals(72911));
			i2++;
		}
		assertTrue(i==1 && i2==1);
		log.info("-----------------End");
*/
    }

	@Test
	@Rollback(true)
	public void isWorkFindMeterVolByKlsk() throws Exception {
		Date dtFrom = Utl.getDateFromStr("01.04.2014");
		Date dtTo = Utl.getDateFromStr("30.04.2014");
		List<SumMeterVol> lstVol = meterDao.findMeterVolUsingKlsk(104882L, dtFrom, dtTo);
		lstVol.forEach(t-> {
				log.info("t.getMeterId()={}, t.getDtFrom={}, t.getDtTo={}, t.getVol()={}",
						t.getMeterId(), t.getDtFrom(), t.getDtTo(), t.getVol());
				}
			);
	}

	/**
	 * Тест запроса на поиск счетчика по коду услуги и объекту Ko
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void isWorkFindTimesampByUser() throws Exception {

		int i=0, i2 = 0;
		log.info("-----------------Begin");
		String period = "201404";
		// найти показания счетчиков
		List<MeterData> lst = meterDao.findMeteringDataTsUsingUser("GIS", "ins_sch", period);
		for (MeterData t : lst) {
			System.out.println(t.getTs()+" "+t.getGuid());
			i++;
		}
		log.info("-----------------End");
	}

	@Test
	public void testLock() throws Exception {
		List<Meter> lst = meterMng.findMeter(72802, 72805);

	}

}
