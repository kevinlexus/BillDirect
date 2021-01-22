import com.dic.app.Config;
import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenPenProcessMng;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.PenCurDAO;
import com.dic.bill.dao.RedirPayDAO;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
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
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import java.math.BigDecimal;
import java.text.ParseException;

/**
 * Тесты формирования задолженности и пени
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@EnableCaching
@DataJpaTest
@Slf4j
public class TestGenPenProcessMng {

    @Autowired
    GenPenProcessMng genPenProcessMng;
    @Autowired
    ConfigApp config;
    @Autowired
    RedirPayDAO redirPayDAO;
    @Autowired
    PenCurDAO penCurDAO;
    @Autowired
    AchargeDAO achargeDAO;

    @Autowired
    private TestDataBuilder testDataBuilder;

    @PersistenceContext
    private EntityManager em;

    @Test
    @Rollback(true)
    @Transactional
    public void testCreateKart() {
        String[] parts = "Петров Виктор Сергеевич".split(" ");
        String fam = null;
        String im = null;
        String ot = null;
        int i=0;
        for (String part : parts) {
            if (i==0){
                fam = part;
            } else if (i==1) {
                im = part;
            } else if (i==2) {
                ot = part;
            }
            i++;
        }

        log.info("fam={}, im={}, ot={}", fam, im, ot);

        StoredProcedureQuery qr;
        qr = em.createStoredProcedureQuery("scott.p_houses.kart_lsk_add");
        qr.registerStoredProcedureParameter("p_lsk_tp", String.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_lsk_new", String.class, ParameterMode.INOUT);
        qr.registerStoredProcedureParameter("p_var", Integer.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_kw", String.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_reu", String.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_house", Integer.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_result", Integer.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_klsk_dst", Integer.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_klsk_premise_dst", Integer.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_fam", String.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_im", String.class, ParameterMode.IN);
        qr.registerStoredProcedureParameter("p_ot", String.class, ParameterMode.IN);
        qr.setParameter("p_lsk_tp", "LSK_TP_MAIN");
        qr.setParameter("p_var", 3);
        qr.setParameter("p_kw", "115");
        qr.setParameter("p_reu", "001");
        qr.setParameter("p_house", 6091);
        qr.setParameter("p_klsk_dst", 104876);
        qr.setParameter("p_klsk_premise_dst", 187);
        qr.setParameter("p_fam", fam);
        qr.setParameter("p_im", im);
        qr.setParameter("p_ot", ot);
        String lsk = qr.getOutputParameterValue("p_lsk_new").toString().trim();
        lsk = lsk.trim();
        log.info("Новый лиц.счет={}", lsk);

        Kart kart = em.find(Kart.class, lsk);
        log.info("check kart.lsk={}", kart.getLsk());
    }

    @Test
    @Rollback(true)
    @Transactional
    public void testGenDebitPen() throws ParseException, ErrorWhileChrgPen {
        log.info("Test GenPenProcessMng.testGenDebitPen - Start");

/*
        Kart kart = em.find(Kart.class, "00000007");
        Ko ko = kart.getKoKw();
*/

        // дом
        House house = new House();
        Ko houseKo = new Ko();
        em.persist(houseKo); // note Используй crud.save

        // добавить вводы
        // без ОДПУ
        // х.в.
        testDataBuilder.addVvodForTest(house, "011", 4, false,
                null, true);
        // г.в.
        testDataBuilder.addVvodForTest(house, "015", 4, false,
                null, true);

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
        String lsk = "РСО_0001";
        Kart kart = em.find(Kart.class, lsk);

        // Добавить входящую задолженность
        testDataBuilder.addDebForTest(kart, "011", 3,
                201401, 201403, 201401, "100.00");
        testDataBuilder.addDebForTest(kart, "011", 3,
                201401, 201403, 201402, "50.00");
        testDataBuilder.addDebForTest(kart, "011", 3,
                201401, 201403, 201403, "20.00");

/*        testDataBuilder.addDebForTest(kart, "003", 1,
                201401, 201403, 201401, "77.84");
        testDataBuilder.addDebForTest(kart, "005", 10,
                201401, 201403, 201401, "0.10");
*/

        // Добавить текущее начисление
        testDataBuilder.addChargeForTest(kart, "011", "70.0");
        testDataBuilder.addChargeForTest(kart, "003", "18.10");
        testDataBuilder.addChargeForTest(kart, "005", "0.12");

        testDataBuilder.addChargeForTest(kart, "029", "8.10");
        testDataBuilder.addChargeForTest(kart, "004", "0.02");
        testDataBuilder.addChargeForTest(kart, "031", "11.10");
        testDataBuilder.addChargeForTest(kart, "013", "23.16");
        testDataBuilder.addChargeForTest(kart, "042", "154.21");
        testDataBuilder.addChargeForTest(kart, "043", "8.17");
        testDataBuilder.addChargeForTest(kart, "038", "555.70");

        // Добавить перерасчеты
        String strDt = "06.04.2014";
        String dopl = "201402";
        ChangeDoc changeDoc = testDataBuilder.buildChangeDocForTest(strDt, dopl);
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "011", 3,
                "201402", "201402", 1, strDt, "-5.0");

        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "011", 3,
                "201311", "201311", 1, strDt, "5.89");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "012", 3,
                "201312", "201312", 1, strDt, "15000.74");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "011", 3,
                "201401", "201401", 1, strDt, "-10.10");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "012", 5,
                "201404", "201404", 1, strDt, "7.11");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "013", 12,
                "201404", "201404", 1, strDt, "3.15");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "006", 8,
                "201404", "201404", 1, strDt, "-33.15");
        testDataBuilder.addChangeForTest(kart, changeDoc, 4, "004", null,
                "201404", "201404", 1, strDt, "-5.90");

        // Добавить платеж
        String dopl2 = "201311";
        Kwtp kwtp = testDataBuilder.buildKwtpForTest(kart, dopl2, "10.04.2014", null, 0,
                "021", "12313", "001", "0000.00", null);

        KwtpMg kwtpMg = testDataBuilder.addKwtpMgForTest(kwtp, dopl2, "20.05", "0.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 3, "20000.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 1, "15.05");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 4, "0.12");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "015", 3, "5.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "006", 8, "10.33");

        kwtpMg = testDataBuilder.addKwtpMgForTest(kwtp, dopl2, "75.08", "0.00");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "003", 1, "50.30");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 3, "19.70");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "011", 4, "5.08");
        testDataBuilder.addKwtpDayForTest(kwtpMg, 1, "005", 4, "22.00");

        // Добавить корректировки оплатой (T_CORRECTS_PAYMENTS)

/*
        ChangeDoc corrPayDoc = testDataBuilder.buildChangeDocForTest(strDt, dopl);
        testDataBuilder.addCorrectPayForTest(kart, corrPayDoc, 4, "011", 3,
                "201401", "201404", "05.04.2014", null, "50.26");
*/

        // построить запрос
        RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                .withTp(1)
                .withGenDt(Utl.getDateFromStr("28.04.2014"))
                .withKo(ko)
                .withCurDt1(config.getCurDt1())
                .withCurDt2(config.getCurDt2())
                .withDebugLvl(1)
                .withRqn(config.incNextReqNum())
                .withIsMultiThreads(false)
                .withStopMark("processMng.process")
                .build();
        reqConf.prepareId();
        reqConf.getCalcStore().setDebugLvl(1);

        // рассчитать задолженность, пеню
        genPenProcessMng.genDebitPen(reqConf.getCalcStore(), true, ko.getId());

        // для того чтобы увидеть insert-ы в тестом режиме
        penCurDAO.findAll().size();

        log.info("Test GenPenProcessMng.testGenDebitPen - End");
    }

}