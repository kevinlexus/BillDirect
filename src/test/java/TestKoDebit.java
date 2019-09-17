import com.dic.app.Config;
import com.dic.app.mm.RegistryMng;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@EnableCaching
@DataJpaTest
@Slf4j
public class TestKoDebit {

    @Autowired
    private RegistryMng registryMng;

    @PersistenceContext
    private EntityManager em;


    public String getNumIndex(String str) {
        System.out.println("str=" + str);
        String trimNum = Utl.ltrim(str, "0");
        //Pattern pattern = Pattern.compile("([/\\\\-].+|[\\d]{1}[^/\\\\-]+)$");
        Pattern pattern = Pattern.compile("([/\\\\-].+|\\d\\p{L}+)$");
        Matcher matcher = pattern.matcher(trimNum);
        if (matcher.find()) {
            return matcher.group(0);
        } else {
            return "";
        }
    }

    /**
     * Проверка корректности расчета начисления по помещению
     */
    @Test
    @Rollback(false)
    @Transactional
    public void printAllDebits() {
        log.info("Test printAllDebits Start");

        int i=1;
        try {
            i--;
            i = 1 / i;
        }catch (ArithmeticException e) {
            java.io.StringWriter sw = new java.io.StringWriter();
            e.printStackTrace(new java.io.PrintWriter(sw));
            String trace = sw.getBuffer().toString();
            log.error("Ошибка! ={}", trace);
        }

        registryMng.genDebitForSberbank();

        log.info("Test printAllDebits End");
    }
}
