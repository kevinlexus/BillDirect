import com.dic.app.Config;
import com.dic.app.mm.RegistryMng;
import com.dic.bill.dao.OrgDAO;
import com.dic.bill.model.scott.Org;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileLoad;
import com.ric.cmn.excp.WrongParam;
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
import java.util.Scanner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestFileLoadingWithDelimiters {


    @PersistenceContext
    private EntityManager em;
    @Autowired
    private RegistryMng registryMng;
    @Autowired
    private OrgDAO orgDAO;


    @Test
    public void testScanner() {
        Scanner scanner = new Scanner("100416574;dfafc63e-f435-46e9-861f-c1cb2886603c;;г Полысаево,,ул Токарева,дом 2,16;1;Услуга вывоза ТКО;112020;207.30");
        scanner.useDelimiter(";");
        while (scanner.hasNext()) {
            String adr = scanner.next();
            Scanner scannerAdr = new Scanner(adr);
            scannerAdr.useDelimiter(",");
            log.info("adr={}", adr);
            int i=0;
            while (scannerAdr.hasNext()) {
                String adrPart = scannerAdr.next();
                log.info("index={}, adrPart={}", i++, adrPart);
            }
        }
    }

    /**
     * Загрузить файл с внешними лиц.счетами во временную таблицу
     */
    @Test
    @Rollback(false)
    public void fileLoadKartExt() throws FileNotFoundException, WrongParam, ErrorWhileLoad {
        // загрузить файл во временную таблицу LOAD_KART_EXT
        Org org = orgDAO.getByReu("001");
        //registryMng.loadFileKartExt(org, "d:\\temp\\#1\\Кап\\0206DBSS001012021.txt");
        registryMng.loadFileKartExt(org, "d:\\temp\\#1\\Кап\\0204DBSS001012021.txt");
    }

    @Test
    @Rollback(false)
    public void approveLoadKartExt() throws WrongParam {
        Org org = orgDAO.getByReu("001");
        // загрузить успешно обработанные лиц.счета в таблицу внешних лиц.счетов
        registryMng.loadApprovedKartExt(org);
    }
    /**
     * Выгрузить файл платежей по внешними лиц.счетами
     */
    @Test
    @Rollback(false)
    public void fileUnloadPaymentKartExt() throws IOException {
        registryMng.unloadPaymentFileKartExt("c:\\temp\\3216613_20200403_.txt", "001",
                Utl.getFirstDt(), Utl.getLastDt());
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
