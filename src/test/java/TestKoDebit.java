import com.dic.app.Config;
import com.dic.app.mm.impl.DebitRegistry;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.PenyaDAO;
import com.dic.bill.mm.EolinkMng;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Kart;
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
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
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
        Path path = Paths.get("c:\\temp\\reestr1.txt");
        // дата формирования
        Date dt = new Date();
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            DebitRegistry debitRegistry = new DebitRegistry();
            for (Kart kart : penyaDAO.getKartWhereDebitExists()) {
                Set<Eolink> eolinks = kart.getEolink();
                for (Eolink eolink : eolinks) {
                    if (eolink.isActual()) {
                        // взять первый актуальный объект лиц.счета
                        final String[] houseFIAS = {null};
                        eolinkMng.getEolinkByEolinkUpHierarchy(eolink, "Дом").ifPresent(t -> {
                            houseFIAS[0] = t.getGuid();
                        });

                        for (Penya penya : kart.getPenya()) {
                            if (Utl.nvl(penya.getSumma(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) != 0) {
                                // есть задолженность
                                debitRegistry.init();
                                debitRegistry.setDelimeter(";");
                                debitRegistry.addElem(
                                        kart.getOwnerFIO(), // ФИО собственника
                                        eolink.getUn(), // ЕЛС
                                        houseFIAS[0], // GUID дома по ФИАС
                                        Utl.ltrim(kart.getNum(), "0"), // № квартиры
                                        kartMng.getAdr(kart), // адрес
                                        kart.getLsk(), // лиц.счет
                                        penya.getSumma().toString() // сумма к оплате
                                );
                                debitRegistry.setDelimeter(";:[!]");
                                debitRegistry.addElem(Utl.getPeriodToMonthYear(penya.getMg1()));

                                // счетчики:
                                List<Meter> lstMeter = meterDAO.findActualByKo(kart.getKoKw().getId(), dt);
                                int i = 0;
                                for (Meter meter : lstMeter) {
                                    i++;
                                    if (meter.getN1() != null) {
                                        // если есть последние показания
                                        debitRegistry.setDelimeter(";");
                                        debitRegistry.addElem(meter.getId().toString());
                                        debitRegistry.addElem(meter.getUsl().getNm2());
                                        if (i == lstMeter.size()) {
                                            debitRegistry.setDelimeter(":[!]");
                                        }
                                        debitRegistry.addElem(meter.getN1().toString());
                                    }
                                }

                                debitRegistry.setDelimeter(";");
                                // услуги:
                                // код
                                debitRegistry.addElem(
                                        kartMng.generateUslNameShort(kart, 2, 3, "_"));
                                // наименование
                                debitRegistry.addElem(
                                        kartMng.generateUslNameShort(kart, 1, 3, ","));
                                // сумма к оплате

                                debitRegistry.addElem(penya.getSumma().toString());
                                if (Utl.nvl(penya.getPenya(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) != 0) {
                                    // есть пеня
                                    // код услуги
                                    debitRegistry.addElem("PEN");
                                    // наименование
                                    debitRegistry.addElem("Пени");
                                }

                                // пустое поле
                                debitRegistry.addElem("");

                                writer.write(debitRegistry.getResult().toString()+"\n");
                                log.info("rec={}", debitRegistry.getResult());
                            }
                        }
                        break;
                    }
                }
            }
        } catch (IOException e) {
            log.error("ОШИБКА! Ошибка записи в файл c:\\temp\\reestr1.txt");
            e.printStackTrace();
        }
        log.info("Test printAllDebits End");
    }
}
