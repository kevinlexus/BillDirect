import com.dic.app.Config;
import com.dic.bill.dao.ApenyaDAO;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Kart;
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
    private ApenyaDAO apenyaDAO;

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
        apenyaDAO.getKoWhereDebitExists("201404").forEach(t->
        {
            for (Kart kart: t.getKart()) {
                Eolink eolink = kart.getEolink();
                log.info("klsk={}, lsk={}, eolink.id={}, ЕЛС={}, ", t.getId(), kart.getLsk(),
                        eolink != null ? eolink.getId() : null,
                        eolink != null ? eolink.getUn() : null);
            }
        }
        );
        log.info("Test printAllDebits End");
    }
}
