import com.dic.app.Config;
import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.NaborMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@EnableCaching
@DataJpaTest
@Slf4j
public class TestKart {

    @Autowired
    private TestDataBuilder testDataBuilder;
    @Autowired
    private KartMng kartMng;
    @Autowired
    private NaborMng naborMng;
    @Autowired
    private ProcessMng processMng;
    @Autowired
    private GenChrgProcessMng genChrgProcessMng;
    @Autowired
    private ConfigApp config;
    @Autowired
    private KartDAO kartDao;

    @PersistenceContext
    private EntityManager em;


    /**
     * Тест запроса на поиск k_lsk_id помещения по параметрам
     */
    @Test
    @Rollback()
    public void isWorkKartMngGetKlskByKulNdKw() throws Exception {

        log.info("-----------------Begin");
        Ko ko = kartMng.getKoPremiseByKulNdKw("0174", "000012", "0000066");
        log.info("Получен klsk={}", ko.getId());
        Assert.assertTrue(ko.getId().equals(105392));

        log.info("-----------------End");
    }

    /**
     * Проверка корректности расчета начисления по помещению
     */
    @Test
    @Rollback()
    @Transactional
    public void genChrgProcessMngGenChrgAppartment() throws WrongParam, ErrorWhileChrg {
        log.info("Test genChrgProcessMngGenChrgAppartment");

        Param param = em.find(Param.class, 1);
        param.setPeriod("201901");
        // конфиг запроса
        RequestConfigDirect reqConf =
                RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                        .withRqn(config.incNextReqNum()) // уникальный номер запроса
                        .withTp(0) // тип операции - начисление
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
        // х.в.
        testDataBuilder.addVvodForTest(house, "011", 4, false,
                null, true);
        // г.в.
        testDataBuilder.addVvodForTest(house, "015", 4, false,
                null, true);

        // построить лицевые счета по помещению
        Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(76.2),
                3, true, true, 1, 1, 547);

        reqConf.setVvod(null);
        reqConf.setKo(ko);
        reqConf.setTp(0);

        // выполнить расчет
        genChrgProcessMng.genChrg(reqConf, ko.getId());

    }

    /**
     * Проверка корректности расчета начисления по дому
     */
    @Test
    @Rollback()
    @Transactional
    public void genChrgProcessMngGenChrgHouse() throws WrongParam, ErrorWhileChrg, ErrorWhileChrgPen, WrongGetMethod, ErrorWhileDist, ErrorWhileGen {
        log.info("Test genChrgProcessMngGenChrgHouse Start!");
        // конфиг запроса
        RequestConfigDirect reqConf =
                null;
        try {
            reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                    .withRqn(config.incNextReqNum()) // уникальный номер запроса
                    .withGenDt(Utl.getDateFromStr("11.04.2014"))
                    .withTp(2) // тип операции - распределение объема
                    .withIsMultiThreads(false) // для Unit - теста однопоточно!
                    .build();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // дом
        House house = new House();
        Ko houseKo = new Ko();

        house.setKo(houseKo);
        house.setKul("0001");
        house.setNd("000001");

        // добавить вводы

        // без ОДПУ
        // Х.в.
/*
        testDataBuilder.addVvodForTest(house, "011", 4, false,
                null, true);

        // Г.в.
        testDataBuilder.addVvodForTest(house, "015", 5, false,
                null, true);
*/

        // с ОДПУ
		// Х.в.
		testDataBuilder.addVvodForTest(house, "011", 1, false,
				new BigDecimal("150.2796"), true);

		// Г.в.
		testDataBuilder.addVvodForTest(house, "015", 1, false,
				new BigDecimal("162.23"), true);

        // Отопление Гкал
        testDataBuilder.addVvodForTest(house, "053", 1, false,
                new BigDecimal("100.25681"), false);

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

        // построить лицевые счета по помещению
        testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(76.2),
                3, true, true, 1, 1, 547);
        testDataBuilder.buildKartForTest(house, "0002", BigDecimal.valueOf(50.24),
                2, true, true, 1, 2, 547);
        testDataBuilder.buildKartForTest(house, "0003", BigDecimal.valueOf(75.89),
                2, true, true, 1, 3, 547);
        // нежилое
        testDataBuilder.buildKartForTest(house, "0004", BigDecimal.valueOf(22.01),
                1, true, true, 9, 1, 547);
        testDataBuilder.buildKartForTest(house, "0005", BigDecimal.valueOf(67.1),
                4, true, true, 1, 2, 547);
        // нормативы по услугам х.в. г.в.
        testDataBuilder.buildKartForTest(house, "0006", BigDecimal.valueOf(35.12),
                2, true, false, 1, 0, 547);

        StopWatch sw = new org.springframework.util.StopWatch();
        sw.start("TIMING:Распределение объемов");

        // ВЫЗОВ распределения объемов
        for (Vvod vvod : house.getVvod()) {
            reqConf.setVvod(vvod);
            //processMng.distVolAll(reqConf); // note отключил... доработать??
        }
        sw.stop();

        reqConf.setVvod(null);
        reqConf.setHouse(house);
        reqConf.setTp(0);
        reqConf.prepareChrgCountAmount();
        reqConf.prepareId();
        sw.start("TIMING:Начисление");
        // вызов начисления
        processMng.processAll(reqConf);
        sw.stop();

        // распечатать объемы

        reqConf.getChrgCountAmount().printVolAmnt(null, "015");
        reqConf.getChrgCountAmount().printVolAmnt(null, "057");
        reqConf.getChrgCountAmount().printVolAmnt(null, "053");
        //calcStore.getChrgCountAmount().printVolAmnt(null, "123");

        //calcStore.getChrgCountAmount().printVolAmnt(null, "003");

/*		calcStore.getChrgCountAmount().printVolAmnt(null, "099");
		calcStore.getChrgCountAmount().printVolAmnt(null, "101");*/


		/*
		calcStore.getChrgCountAmount().printVolAmnt(null, "053");
*/
        // распечатать C_CHARGE
        reqConf.getChrgCountAmount().printChrg(em.find(Kart.class, "ОСН_0001"));
        reqConf.getChrgCountAmount().printChrg(em.find(Kart.class, "РСО_0001"));
        log.info(sw.prettyPrint());
        log.info("Test genChrgProcessMngGenChrgHouse End!");
    }


    @Test
    @Rollback()
    @Transactional
    public void testVvod() {
        Vvod vvod = em.find(Vvod.class, 6050);
        List<Long> lstItem = kartMng.getKoByVvod(vvod).stream().map(Ko::getId).collect(Collectors.toList());
        for (Long id : lstItem) {
            log.info("check id={}", id);
        }
    }

    @Test
    public void testBD() {
        BigDecimal tt = new BigDecimal("55.23");
        BigDecimal tt2 = new BigDecimal("55.23");
        BigDecimal tt3 = tt.multiply(tt2).setScale(2, BigDecimal.ROUND_HALF_UP);
        log.info("check={}", tt3);
    }

    @Test
    public void testCache() {
        try {
            log.info("check1={}",
            naborMng.getCached("bla1", 2, Utl.getDateFromStr("01.01.2019")));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        try {
            log.info("check2={}",
            naborMng.getCached("bla1", 2, Utl.getDateFromStr("01.01.2019")));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoadKart() {
        log.info("check1");
        Kart kart = em.find(Kart.class, "00000381");
        //Kart kart = kartDao.findByLsk("00000007");
        Optional<Kart> optKart = kartDao.findById("00000007");
        //if (optKart.isPresent()) {
          //  Kart kart = optKart.get();
            //log.info(kart.getOwnerFIO());
            //log.info(kart.getKul());
            //log.info(kart.getNd());
        //}
        log.info("check3={}", kart);
        log.info("check2");
    }

}
