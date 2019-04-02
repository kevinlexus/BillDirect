import com.dic.app.Config;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Date;

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

    @PersistenceContext
    private EntityManager em;


    /**
     * Проверка корректности распределения платежа (Кис)
     */
    @Test
    @Rollback()
    @Transactional
    public void testDistPay() {
        log.info("Test TestDistPay.testDistPay");
        final String lsk = "00000004";
        Kart kart = em.find(Kart.class, lsk);


        // Добавить наборы
        kart.getNabor().clear();
        testDataBuilder.addNaborForTest(kart, 7, "003", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 4, "005", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 12, "004", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 8, "006", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 4, "004", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 3, "007", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 1, "019", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 11, "014", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 13, "031", null, null, null, null, null);
        testDataBuilder.addNaborForTest(kart, 9, "015", null, null, null, null, null);

        // Добавить сальдо
        kart.getSaldoUsl().clear();

        // прошлый период
        testDataBuilder.addSaldoUslForTest(kart, 7, "003", "201403", "100.50");
        testDataBuilder.addSaldoUslForTest(kart, 8, "004", "201403", "100.50");
        testDataBuilder.addSaldoUslForTest(kart, 9, "004", "201403", "-200.50");
        testDataBuilder.addSaldoUslForTest(kart, 1, "004", "201403", "-100.50");

        // текущий период
        testDataBuilder.addSaldoUslForTest(kart, 7, "003", "201404", "200.50");
        testDataBuilder.addSaldoUslForTest(kart, 4, "005", "201404", "22.53");
        testDataBuilder.addSaldoUslForTest(kart, 12, "004", "201404", "0.11");
        testDataBuilder.addSaldoUslForTest(kart, 8, "006", "201404", "10.34");
        testDataBuilder.addSaldoUslForTest(kart, 4, "004", "201404", "3.79");
        testDataBuilder.addSaldoUslForTest(kart, 3, "007", "201404", "4.18");
        testDataBuilder.addSaldoUslForTest(kart, 1, "019", "201404", "-50.19");
        testDataBuilder.addSaldoUslForTest(kart, 11, "014", "201404", "-100.79");
        testDataBuilder.addSaldoUslForTest(kart, 13, "031", "201404", "-1");
        testDataBuilder.addSaldoUslForTest(kart, 9, "015", "201404", "-8.38");

        // Добавить начисление
        kart.getCharge().clear();
        testDataBuilder.addChargeForTest(kart, "011", "8.10");
        testDataBuilder.addChargeForTest(kart, "003", "18.10");
        testDataBuilder.addChargeForTest(kart, "004", "0.12");
        testDataBuilder.addChargeForTest(kart, "007", "11.10");
        testDataBuilder.addChargeForTest(kart, "031", "23.16");
        testDataBuilder.addChargeForTest(kart, "006", "154.21");
        testDataBuilder.addChargeForTest(kart, "019", "8.17");
        testDataBuilder.addChargeForTest(kart, "015", "0.70");

        // Добавить перерасчеты
        kart.getChange().clear();
        Date dtek = Utl.getDateFromStr("01.04.2014");
        ChangeDoc changeDoc = new ChangeDoc();
        changeDoc.setDt(dtek);
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, 4, "011",
                "201404", null, 1, dtek, "118.10");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, 5, "012",
                "201404", null, 1, dtek, "7.11");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, 12, "013",
                "201404", null, 1, dtek, "3.15");

        BigDecimal itgChrg = kart.getSaldoUsl().stream()
                .filter(t->t.getMg().equals("201404"))
                .map(SaldoUsl::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого начисление:{}", itgChrg);
        BigDecimal itgChng = kart.getChange().stream()
                .filter(t->t.getKart().getLsk().equals(lsk))
                .map(Change::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого перерасчеты:{}", itgChng);
    }


}
