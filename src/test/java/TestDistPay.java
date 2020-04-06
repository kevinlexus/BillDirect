import com.dic.app.Config;
import com.dic.app.mm.DistPayMng;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.excp.ErrorWhileDistPay;
import com.ric.cmn.excp.WrongParam;
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
    private AchargeDAO achargeDAO;
    @Autowired
    private SaldoMng saldoMng;
    @Autowired
    private SaldoUslDAO saldoUslDAO;
    @Autowired
    private DistPayMng distPayMng;

    @PersistenceContext
    private EntityManager em;

    @Test
    public void check() {
        String some = null;
        if (1 == 1) {
            some = "11";
        }
        String check = Preconditions.checkNotNull(some);
        log.info("check:{}", check);
    }

    @Test
    @Rollback()
    @Transactional
    public void testGetPinSalXitog3ByLsk() throws ErrorWhileDistPay, WrongParam {
        log.info("Test saldoUslDAO.getPinSalXitog3ByLsk");
        List<SumUslOrgRec> lst = saldoUslDAO.getPinSalXitog3ByLsk("00000202", "201403");
        lst.forEach(t -> {
            log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        });
    }

    @Test
    @Rollback()
    @Transactional
    public void testGetCreditSaldoUslWhereDebitExists() throws ErrorWhileDistPay, WrongParam {
        log.info("Test saldoUslDAO.getSaldoUslWhereCreditAndDebitExists");
        List<SaldoUsl> lst = saldoUslDAO.getSaldoUslWhereCreditAndDebitExists("201405");
        lst.forEach(t -> {
            log.info("lsk={}, usl={}, org={}, summa={}",
                    t.getKart().getLsk(), t.getUsl().getId(), t.getOrg().getId(), t.getSumma());
        });
    }

    /**
     * Проверка корректности распределения платежа (Кис)
     */
    @Test
    @Rollback()
    @Transactional
    public void testDistPay() throws ErrorWhileDistPay, WrongParam {
        log.info("Test TestDistPay.testDistPay");

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
        //int ukId = 547; // общий тип распределения
        Ko ko = testDataBuilder.buildKartForTest(house, "0001", BigDecimal.valueOf(76.2),
                3, true, true, 1, 1, ukId);
        em.persist(ko); // note Используй crud.save
        String lsk = "ОСН_0001";
        Kart kart = em.find(Kart.class, lsk);

        // Добавить сальдо
        // прошлый период
        testDataBuilder.buildSaldoUslForTest(kart, "003", 7, "201403", "71.48");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 8, "201403", "0.02");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 9, "201403", "3.17");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 1, "201403", "775.25");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 2, "201403", "77.37");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 3, "201403", "9.57");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 4, "201403", "-0.01");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 5, "201403", "0.01");

        // текущий период
/*
        testDataBuilder.buildSaldoUslForTest(kart, "003", 7, "201404", "200.50");
        testDataBuilder.buildSaldoUslForTest(kart, "005", 4, "201404", "22.53");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 12, "201404", "0.11");
        testDataBuilder.buildSaldoUslForTest(kart, "006", 8, "201404", "1089.34");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 4, "201404", "3.79");
        testDataBuilder.buildSaldoUslForTest(kart, "007", 3, "201404", "4.18");
        testDataBuilder.buildSaldoUslForTest(kart, "005", 8, "201404", "100");
*/

        testDataBuilder.buildSaldoUslForTest(kart, "003", 7, "201404", "71.48");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 8, "201404", "0.02");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 9, "201404", "3.17");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 1, "201404", "775.25");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 2, "201404", "77.37");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 3, "201404", "9.57");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 4, "201404", "-0.01");
        testDataBuilder.buildSaldoUslForTest(kart, "004", 5, "201404", "0.01");

        // Добавить текущее начисление
        testDataBuilder.addChargeForTest(kart, "029", "8.10");
        testDataBuilder.addChargeForTest(kart, "003", "18.10");
        testDataBuilder.addChargeForTest(kart, "004", "0.02");
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
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "011", 4,
                "201404", "201404", 1, strDt, "118.10");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "012", 5,
                "201404", "201404", 1, strDt, "7.11");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "013", 12,
                "201404", "201404", 1, strDt, "3.15");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "006", 8,
                "201404", "201404", 1, strDt, "-33.15");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "004", null,
                "201404", "201404", 1, strDt, "-5.90");

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
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "006", 8, "10.33");

        kwtpMg = testDataBuilder.addKwtpMgForTest(kwtp, dopl, "75.08", "0.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 1, "50.30");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 4, "19.70");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 4, "5.08");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "005", 4, "22.00");

        log.info("Тест-записи - Nabor: Набор услуг:");
        kart.getNabor().forEach(t -> log.info("usl={}, org={}", t.getUsl().getId(), t.getOrg().getId()));

        log.info("Тест-записи - SaldoUsl: Вх.сальдо:");
        List<SumUslOrgDTO> lst = saldoMng.getOutSal(kart, "201404", null, null, true,
                false, false, false, false, false, null, false, false);
        for (SumUslOrgDTO t : lst) {
            log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        }

        log.info("Тест-записи - Charge: Начисление:");
        kart.getCharge().forEach(t -> {
            log.info("usl={}, summa={}", t.getUsl().getId(), t.getSumma());
        });
        BigDecimal itgChrg = kart.getCharge().stream()
                .map(Charge::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого начисление:{}", itgChrg);

        log.info("Тест-записи - Change: Перерасчеты:");
        kart.getChange().forEach(t -> {
            log.info("usl={}, org={}, summa={}", t.getUsl().getId(),
                    t.getOrg() != null ? t.getOrg().getId() : null, t.getSumma());
        });
        BigDecimal itgChng = kart.getChange().stream()
                .map(Change::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого перерасчеты:{}", itgChng);

        log.info("Тест-записи - V_CHANGES_FOR_SALDO: Контроль перерасчетов:");
        lst = saldoMng.getOutSal(kart, "201404", null, null,
                false, false, false, true, false, false, null, false, false);
        for (SumUslOrgDTO t : lst) {
            log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        }

        log.info("Тест-записи - CorrectPay: Корректировки оплаты:");
        kart.getCorrectPay().forEach(t -> {
            log.info("usl={}, org={}, summa={}", t.getUsl().getId(), t.getOrg().getId(), t.getSumma());
        });
        BigDecimal itgCorrPay = kart.getCorrectPay().stream()
                .map(CorrectPay::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого корректировки оплаты:{}", itgCorrPay);

        log.info("Тест-записи - KwtpDay: Оплата:");
        kart.getKwtpDay().forEach(t -> {
            log.info("usl={}, org={}, summa={}", t.getUsl().getId(), t.getOrg().getId(), t.getSumma());
        });
        BigDecimal itgPay = kart.getKwtpDay().stream()
                .map(KwtpDay::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого оплата KwtpDay:{}", itgPay);

        log.info("Тест-записи - SaldoUsl: Исх.сальдо:");
        lst = saldoMng.getOutSal(kart, "201404", null, null,
                true, true, false, true, true, true, null, false, false);
        for (SumUslOrgDTO t : lst) {
            log.info("usl={}, org={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        }

        BigDecimal itgSumma = kart.getKwtpMg().stream()
                .map(KwtpMg::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal itgPen = kart.getKwtpMg().stream()
                .map(KwtpMg::getPenya).reduce(BigDecimal.ZERO, BigDecimal::add);
        log.info("Итого оплата KwtpMg:summa={}, pay={}", itgSumma, itgPen);

        itgPay = kart.getKwtp().stream()
                .map(Kwtp::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add);


        log.info("Итого оплата Kwtp:{}", itgPay);

        // Добавить платеж для распределения
        log.info("");
        String strSumma = "936.86";
        String strPenya = "25.87";
        String strDebt = "90.87";
        log.info("Распределить сумму:{}", strSumma);
        String dopl2 = "201402";
/*
        kwtp = testDataBuilder.buildKwtpForTest(kart, dopl2, "11.04.2014", null, 0,
                "022", "12314", "001", strSumma, strPenya);
        kwtpMg = testDataBuilder.addKwtpMgForTest(kwtp, dopl2, strSumma, strPenya);
*/

        // распределить
        distPayMng.distKwtpMg(1111111, kart.getLsk(), strSumma, strPenya, strDebt,
                dopl2, 0, "011", "001",
                "11.04.2014", null, true);
    }

    @Test
    @Rollback()
    @Transactional
    public void testDistSalCorrOperation() {
        log.info("Test DistPayMng.distSalCorrOperation");
        distPayMng.distSalCorrOperation();
    }

}