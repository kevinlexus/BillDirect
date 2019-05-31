import com.dic.app.Config;
import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.app.mm.GenPenProcessMng;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.RedirPayDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.mm.TestDataBuilder;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
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
import java.text.ParseException;
import java.util.List;

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

    @PersistenceContext
    private EntityManager em;

    @Test
    @Rollback(false)
    @Transactional
    public void testGenDebitPen() throws ParseException, ErrorWhileChrgPen {
        log.info("Test GenPenProcessMng.testGenDebitPen - Start");

        // построить запрос
        Ko ko = em.find(Ko.class, 104880L);
        RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                .withTp(1)
                .withGenDt(Utl.getDateFromStr("30.04.2014"))
                .withKo(ko)
                .withCurDt1(config.getCurDt1())
                .withCurDt2(config.getCurDt2())
                .withDebugLvl(1)
                .withRqn(config.incNextReqNum())
                .withIsMultiThreads(false)
                .withStopMark("processMng.genProcess")
                .build();
        reqConf.prepareId();
        reqConf.getCalcStore().setDebugLvl(1);
        genPenProcessMng.genDebitPen(reqConf.getCalcStore(), true, 104880L);

        log.info("Test GenPenProcessMng.testGenDebitPen - End");
    }

}