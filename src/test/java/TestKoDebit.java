import com.dic.app.Config;
import com.dic.bill.dao.PenyaDAO;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.exs.Eolink;
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

                    log.info("lsk={}, fio={}, els={}, houseFIAS={}", kart.getLsk(), kart.getOwnerFIO(),
                            eolink.getUn(), houseFIAS[0]);
                    break;
                }
            }
        }
        );
        log.info("Test printAllDebits End");
    }
}
