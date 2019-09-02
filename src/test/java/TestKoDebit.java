import com.dic.app.Config;
import com.dic.app.mm.impl.DebitRegistry;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.PenyaDAO;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Meter;
import com.dic.bill.model.scott.Penya;
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
import java.util.Optional;
import java.util.Set;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@EnableCaching
@DataJpaTest
@Slf4j
public class TestKoDebit {

    @Autowired
    private KartMng kartMng;
    @Autowired
    private EolinkMng eolinkMng;
    @Autowired
    private PenyaDAO penyaDAO;
    @Autowired
    private MeterDAO meterDAO;

    @PersistenceContext
    private EntityManager em;


    /**
     * Проверка корректности расчета начисления по помещению
     */
    @Test
    @Rollback()
    @Transactional
    public void printAllDebits() {
        log.info("Test printAllDebits Start");

        // дата формирования
        Date dt = new Date();
        penyaDAO.getKartWhereDebitExists().forEach(kart->
        {
            Set<Eolink> eolinks = kart.getEolink();
            for (Eolink eolink : eolinks) {
                if (eolink.isActual()) {
                    // взять первый актуальный объект лиц.счета
                    final String[] houseFIAS = {null};
                    eolinkMng.getEolinkByEolinkUpHierarchy(eolink, "Дом").ifPresent(t->{
                            houseFIAS[0] = t.getGuid();
                    });

                    for (Penya penya : kart.getPenya()) {
                        if (Utl.nvl(penya.getSumma(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) !=0) {
                            // есть задолженность
                            DebitRegistry debitRegistry = new DebitRegistry();
                            debitRegistry.setDelimeter(";");
                            debitRegistry.addElem(kart.getLsk(), kart.getOwnerFIO(),
                                    eolink.getUn(), houseFIAS[0], Utl.ltrim(kart.getNum(), "0"), kartMng.getAdr(kart),
                                    kart.getLsk(), penya.getSumma().toString());
                            debitRegistry.setDelimeter(";:[!]");
                            debitRegistry.addElem(Utl.getPeriodToMonthYear(penya.getMg1()));

                            // добавить счетчики
                            for (Meter meter : meterDAO.findActualByKo(kart.getKoKw().getId(), dt)) {
                                debitRegistry.setDelimeter(";");
                                debitRegistry.addElem(meter.getId().toString());
                                debitRegistry.addElem(meter.getUsl().getNm2());
                            }

                            log.info("rec={}", debitRegistry.getResult());

/*
                            log.info("lsk={}, fio={}, els={}, houseFIAS={}, кв={}, адр={}, lsk={}, задолж={}, период={}," +
                                            "код.сч={}, наим.сч={}, код.усл={}",
                                    kart.getLsk(), kart.getOwnerFIO(),
                                    eolink.getUn(), houseFIAS[0], Utl.ltrim(kart.getNum(), "0"), kartMng.getAdr(kart),
                                    kart.getLsk(), penya.getSumma(), penya.getMg1(),
                                    "КОД.СЧ.", "НАИМ.СЧ", "КОД.УСЛ.", "НАИМ.УСЛ", penya.getSumma());
*/
                        }
                        if (Utl.nvl(penya.getPenya(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) !=0) {
                            // есть пеня

                        }

                    }
                    break;
                }
            }
        }
        );
        log.info("Test printAllDebits End");
    }
}
