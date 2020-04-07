import com.dic.app.Config;
import com.dic.app.mm.RegistryMng;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.FileNotFoundException;
import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestFileLoadingWithDelimiters {


	@PersistenceContext
    private EntityManager em;
	@Autowired
    private RegistryMng registryMng;

    /**
     * Загрузить файл с внешними лиц.счетами во временную таблицу
     */
    @Test
    @Rollback(false)
    public void fileLoadKartExt() throws FileNotFoundException {
        // загрузить файл во временную таблицу LOAD_KART_EXT
        registryMng.loadFileKartExt("г Полысаево", "001", "LSK_TP_MAIN",
                "d:\\temp\\#46\\1.txt");
        // загрузить успешно обработанные лиц.счета в таблицу внешних лиц.счетов
        registryMng.loadApprovedKartExt();
    }

    /**
     * Выгрузить файл платежей по внешними лиц.счетами
     */
    @Test
    @Rollback(false)
    public void fileUnloadPaymentKartExt() throws IOException {
        registryMng.unloadPaymentFileKartExt("c:\\temp\\3216613_20200403_.txt", "001");
    }

    /**
     * Загрузить файл с показаниями по счетчикам
     */
    @Test
    @Rollback(false)
    public void fileLoadMeterVal() throws FileNotFoundException {
        registryMng.loadFileMeterVal("d:\\temp\\#46\\Форма для подачи показаний х.в,г.в. в элек.виде.csv",
                "windows-1251", false);
    }

}
