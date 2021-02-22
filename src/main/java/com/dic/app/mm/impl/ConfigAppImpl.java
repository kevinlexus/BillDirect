package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.bill.Lock;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.model.scott.Param;
import com.dic.bill.model.scott.Tuser;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.text.ParseException;
import java.util.*;

/**
 * Конфигуратор приложения
 *
 * @author lev
 * @version 1.01
 */
@Service
@Slf4j
public class ConfigAppImpl implements ConfigApp {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private TuserDAO tuserDAO;
    // номер текущего запроса
    private int reqNum = 0;

    // прогресс текущего формирования
    private Integer progress;

    // блокировщик выполнения процессов
    private Lock lock;

    @PostConstruct
    private void setUp() {
        log.info("");
        log.info("-----------------------------------------------------------------");
        log.info("Версия модуля - {}", "1.0.11");
        log.info("Начало расчетного периода = {}", getCurDt1());
        log.info("Конец расчетного периода = {}", getCurDt2());
        log.info("-----------------------------------------------------------------");
        log.info("");

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+7"));
        // блокировщик процессов
        setLock(new Lock());
    }

    /**
     * Проверка необходимости выйти из приложения

    @Scheduled(fixedDelay = 2000)
    public void checkTerminate() {
        // проверка файла "stop" на завершение приложения (для обновления)
        File tempFile = new File("stop");
        boolean exists = tempFile.exists();
        if (exists) {
            log.info("ВНИМАНИЕ! ЗАПРОШЕНА ОСТАНОВКА ПРИЛОЖЕНИЯ! - БЫЛ СОЗДАН ФАЙЛ c:\\Progs\\GisExchanger\\stop");
            SpringApplication.exit(ctx, () -> 0);
        }
    }
     */
    // Получить Calendar текущего периода
    private List<Calendar> getCalendarCurrentPeriod() {
        List<Calendar> calendarLst = new ArrayList<>();

        Param param = em.find(Param.class, 1);
        if (param == null) {
            log.error("ВНИМАНИЕ! Установить SCOTT.PARAMS.ID=1");
        }

        Calendar calendar1, calendar2;
        calendar1 = new GregorianCalendar();
        calendar1.clear(Calendar.ZONE_OFFSET);

        calendar2 = new GregorianCalendar();
        calendar2.clear(Calendar.ZONE_OFFSET);


        // получить даты начала и окончания периода
        assert param != null;
        Date dt = null;
        try {
            dt = Utl.getDateFromPeriod(param.getPeriod().concat("01"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date dt1 = Utl.getFirstDate(dt);
        Date dt2 = Utl.getLastDate(dt1);

        calendar1.setTime(dt1);
        calendarLst.add(calendar1);

        calendar2.setTime(dt2);
        calendarLst.add(calendar2);

        return calendarLst;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String getPeriod() {
        return Utl.getPeriodFromDate(getCalendarCurrentPeriod().get(0).getTime());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Tuser getCurUser() {
        return tuserDAO.findByCd("GEN");
    }

    @Override
    public String getPeriodNext() {
        try {
            return Utl.addMonths(Utl.getPeriodFromDate(getCalendarCurrentPeriod().get(0).getTime()), 1);
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            return null;
        }
    }

    @Override
    public String getPeriodBack() {
        try {
            return Utl.addMonths(Utl.getPeriodFromDate(getCalendarCurrentPeriod().get(0).getTime()), -1);
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            return null;
        }
    }

    /**
     * Получить первую дату текущего месяца
     */
    @Override
    public Date getCurDt1() {
        return getCalendarCurrentPeriod().get(0).getTime();
    }

    /**
     * Получить последнюю дату текущего периода
     */
    @Override
    public Date getCurDt2() {
        return getCalendarCurrentPeriod().get(1).getTime();
    }

    // получить следующий номер запроса
    @Override
    public synchronized int incNextReqNum() {
        return this.reqNum++;
    }

    @Override
    public Lock getLock() {
        return lock;
    }

    private void setLock(Lock lock) {
        this.lock = lock;
    }

    @Override
    public Integer getProgress() {
        return progress;
    }

    @Override
    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    @Override
    public void incProgress() {
        progress++;
    }

}
