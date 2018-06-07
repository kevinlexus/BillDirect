import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dic.app.Config;
import com.dic.app.mm.MigrateMng;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistDeb;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestMigration {


	@PersistenceContext
    private EntityManager em;
	@Autowired
    private MigrateMng migrateMng;

    @Test
    @Rollback(false)
    public void mainTestMigration() {
		log.info("Test start");

		try {
			migrateMng.migrateDeb("00000085", 201403);
		} catch (ErrorWhileDistDeb e) {
			// TODO Auto-generated catch block
			log.error(Utl.getStackTraceString(e));
		}

		log.info("Test end");
	}


}
