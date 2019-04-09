import com.dic.app.Config;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Preconditions;
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

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.List;

/**
 * Тесты распределения оплаты
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@EnableCaching
@DataJpaTest
@Slf4j
public class TestDistPay {

    @Autowired
    private TestDataBuilder testDataBuilder;
    @Autowired
    private SaldoUslDAO saldoUslDao;
    @Autowired
    private CorrectPayDAO correctPayDAO;
    @Autowired
    private SaldoMng saldoMng;

    @PersistenceContext
    private EntityManager em;

    @Test
    public void check() {
        String some = null;
        if (1==1) {
            some = "11";
        }
        String check = Preconditions.checkNotNull(some);
        log.info("check:{}", check);
    }

    /**
     * Проверка корректности распределения платежа (Кис)
     */
    @Test
    @Rollback()
    @Transactional
    public void testDistPay() {
        log.info("Test TestDistPay.testDistPay");
        // дом
        House house = new House();
        Ko houseKo = new Ko();

        house.setKo(houseKo);
        house.setKul("0001");
        house.setNd("000001");

        // построить лицевые счета по помещению
        Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(76.2),
                3, true, true, 1, 1);
        String lsk = "ОСН_0001";
        Kart kart = em.find(Kart.class, lsk);

        // Добавить сальдо
        // прошлый период
        testDataBuilder.buildSaldoUslForTest(kart, 7, "003", "201403", "10.62");
        testDataBuilder.buildSaldoUslForTest(kart, 8, "004", "201403", "552.17");
        testDataBuilder.buildSaldoUslForTest(kart, 9, "004", "201403", "-211.88");
        testDataBuilder.buildSaldoUslForTest(kart, 1, "004", "201403", "-14.25");

        // текущий период
        testDataBuilder.buildSaldoUslForTest(kart, 7, "003", "201404", "200.50");
        testDataBuilder.buildSaldoUslForTest(kart, 4, "005", "201404", "22.53");
        testDataBuilder.buildSaldoUslForTest(kart, 12, "004", "201404", "0.11");
        testDataBuilder.buildSaldoUslForTest(kart, 8, "006", "201404", "10.34");
        testDataBuilder.buildSaldoUslForTest(kart, 4, "004", "201404", "3.79");
        testDataBuilder.buildSaldoUslForTest(kart, 3, "007", "201404", "4.18");
        testDataBuilder.buildSaldoUslForTest(kart, 1, "019", "201404", "-50.19");
        testDataBuilder.buildSaldoUslForTest(kart, 11, "014", "201404", "-100.79");
        testDataBuilder.buildSaldoUslForTest(kart, 10, "031", "201404", "-1");
        testDataBuilder.buildSaldoUslForTest(kart, 9, "015", "201404", "-8.38");

        // Добавить начисление
        testDataBuilder.addChargeForTest(kart, "029", "8.10");
        testDataBuilder.addChargeForTest(kart, "003", "18.10");
        testDataBuilder.addChargeForTest(kart, "005", "0.12");
        testDataBuilder.addChargeForTest(kart, "031", "11.10");
        testDataBuilder.addChargeForTest(kart, "011", "55.5");
        testDataBuilder.addChargeForTest(kart, "013", "23.16");
        testDataBuilder.addChargeForTest(kart, "042", "154.21");
        testDataBuilder.addChargeForTest(kart, "043", "8.17");
        testDataBuilder.addChargeForTest(kart, "038", "555.70");

        // Добавить перерасчеты
        String strDt = "01.04.2014";
        String dopl = "201401";
        ChangeDoc changeDoc = testDataBuilder.buildChangeDocForTest(strDt, dopl);
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, 4, "011",
                "201404", null, 1, strDt, "118.10");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, 5, "012",
                "201404", null, 1, strDt, "7.11");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, 12, "013",
                "201404", null, 1, strDt, "3.15");

        // Добавить корректировки оплатой (T_CORRECTS_PAYMENTS)
        ChangeDoc corrPayDoc = testDataBuilder.buildChangeDocForTest(strDt, dopl);
        testDataBuilder.addCorrectPayForTest(kart, corrPayDoc, 4, "011", 4,
                "201401", "201404", "10.04.2014", null, "50.26");

        // Добавить платеж
        Kwtp kwtp = testDataBuilder.buildKwtpForTest(kart, dopl, "10.04.2014", null, 0,
                "021", "12313", "001", "100.25", null);
        KwtpMg kwtpMg = testDataBuilder.addKwtpMgForTest(kwtp, dopl, "20.05", "5.12");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 1, "10.05");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 4, "10.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "015", 3, "5.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 4, "0.12");

        kwtpMg = testDataBuilder.addKwtpMgForTest(kwtp, dopl, "75.08", "0.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 1, "50.30");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 4, "19.70");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 4, "5.08");

        kart.getNabor().forEach(t-> log.info("Nabor: usl={}, org={}", t.getUsl().getId(), t.getOrg().getId()));
        log.info("");

        BigDecimal itgChrg = kart.getCharge().stream()
                .map(Charge::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого начисление:{}", itgChrg);

        BigDecimal itgChng = kart.getChange().stream()
                .map(Change::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого перерасчеты:{}", itgChng);

        BigDecimal itgCorrPay = kart.getCorrectPay().stream()
                .map(CorrectPay::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого корректировки оплаты:{}", itgCorrPay);

        List<SumUslOrgRec> lstSal = saldoUslDao.getSaldoUslByLsk(lsk, "201403");
        BigDecimal itgSal = lstSal.stream().map(SumUslOrgRec::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого сальдо:{}", itgSal);

        BigDecimal itgPay = kart.getKwtpDay().stream()
                .map(KwtpDay::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого оплата KwtpDay:{}", itgPay);

        BigDecimal itgSumma = kart.getKwtpMg().stream()
                .map(KwtpMg::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal itgPen = kart.getKwtpMg().stream()
                .map(KwtpMg::getPenya).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого оплата KwtpMg:summa={}, pay={}", itgSumma, itgPen);

        itgPay = kart.getKwtp().stream()
                .map(Kwtp::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого оплата Kwtp:{}", itgPay);

/*
        List<SumUslOrgDTO> lst = saldoMng.getOutSal(kart, "201404",
                true, true, true, true, true);
*/
        List<SumUslOrgDTO> lst = saldoMng.getOutSal(kart, "201404",
                true, true, true, true, true);
        for (SumUslOrgDTO t : lst) {
            log.info("Исходящее сальдо: usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        }
    }
}
