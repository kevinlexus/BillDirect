package com.dic.app;

import com.dic.app.mm.ConfigApp;
import com.dic.bill.SpringContext;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dao.VvodDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.ChrgCountAmount;
import com.dic.bill.mm.KartMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

//import com.dic.bill.model.scott.SessionDirect;

/**
 * Конфигуратор запроса
 *
 * @author Lev
 */
@Getter
@Setter
@Slf4j
public class RequestConfigDirect implements Cloneable {

    // Id запроса
    int rqn;
    // тип выполнения 0-начисление, 3 - начисление для распределения по вводу, 1 - задолженность и пеня, 2 - распределение объемов по вводу,
    // 4 - начисление по одной услуге, для автоначисления - по нормативу
    int tp = 0;
    // уровень отладки
    int debugLvl = 0;
    // дата на которую сформировать
    Date genDt = null;

    // текущий период
    Date curDt1;
    Date curDt2;

    // выполнять многопоточно
    boolean isMultiThreads = false;
    // кол-во потоков
    int cntThreads = 1;
    final int CNT_THREADS_FOR_COMMON_TASKS = 7; // если сделать больше 10 то виснет на домашнем компе... почему??? ред.21.03.2019
    // объекты формирования:
    // УК
    Org uk = null;
    // дом
    House house = null;
    // ввод
    Vvod vvod = null;
    // помещение
    Ko ko = null;
    // выбранный тип объекта формирования
    private int tpSel;
    // блокировать для выполнения длительного процесса?
    private boolean isLockForLongLastingProcess = false;

    // услуга
    Usl usl = null;

    // список Id объектов формирования
    List<Long> lstItems;

    // хранилище справочников
    CalcStore calcStore;

    // хранилище объемов по вводу (дому) (для ОДН и прочих нужд)
    private ChrgCountAmount chrgCountAmount;

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    // получить наименование типа выполнения
    public String getTpName() {
        switch (this.tp) {
            case 0:
                return "Начисление";
            case 1:
                return "Задолженность и пеня";
            case 2:
                return "Распределение объемов";
            case 3:
                return "Начисление для распределения объемов";
            case 4:
                return "Начисление по одной услуге, для автоначисления";
        }
        return null;
    }

    /**
     * Проверить параметры запроса
     *
     * @return - null -  нет ошибок, !null - описание ошибки
     */
    public String checkArguments() {
        switch (this.tp) {
            case 1:
            case 2:
            case 3:
            case 4:
                {
                // задолженность и пеня, - проверить текущую дату
                if (genDt == null) {
                    return "ERROR! некорректная дата расчета!";
                } else {
                    // проверить, что дата в диапазоне текущего периода
                    if (!Utl.between(genDt, curDt1, curDt2)) {
                        return "ERROR! дата не находится в текущем периоде genDt=" + genDt;
                    }
                }
                break;
            }
        }
        switch (this.tp) {
            case 4: {
                if (this.usl == null) {
                    return "ERROR! не заполнена услуга (uslId) для расчета!";
                }
                break;
            }
        }

        return null;
    }


    /**
     * Подготовка списка Id (помещений, вводов)
     */
    public void prepareId() {
        if (Utl.in(tp, 0,3,4)) {
            // начисление, начисление для распределения объемов, начисление по одной услуге, для автоначисления
            KartDAO kartDao = SpringContext.getBean(KartDAO.class);
            if (ko != null) {
                // по помещению
                isLockForLongLastingProcess = false;
                setTpSel(1);
                lstItems = new ArrayList<>(1);
                lstItems.add(ko.getId());
                cntThreads=1;
            } else if (uk != null) {
                // по УК
                isLockForLongLastingProcess = true;
                setTpSel(4); // почему 0 как и по всему фонду???
                lstItems = kartDao.findAllKlskIdByReuId(uk.getReu())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            } else if (house != null) {
                // по дому
                isLockForLongLastingProcess = false;
                setTpSel(3);
                //lstItems = kartMng.getKoByHouse(house).stream().map(Ko::getId).collect(Collectors.toList());
                lstItems = kartDao.findAllKlskIdByHouseId(house.getId())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            } else if (vvod != null) {
                // по вводу
                isLockForLongLastingProcess = false;
                setTpSel(2);
                //lstItems = kartMng.getKoByVvod(vvod).stream().map(Ko::getId).collect(Collectors.toList());
                lstItems = kartDao.findAllKlskIdByVvodId(vvod.getId())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            } else {
                isLockForLongLastingProcess = true;
                setTpSel(5);
                // по всему фонду
                // конвертировать из List<BD> в List<Long> (native JPA представляет k_lsk_id только в BD и происходит type Erasure)
                lstItems = kartDao.findAllKlskId()
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            }
        } else if (tp == 2) {
            // распределение вводов
            VvodDAO vvodDAO = SpringContext.getBean(VvodDAO.class);
            if (vvod != null) {
                // распределить конкретный ввод
                lstItems = new ArrayList<>(1);
                lstItems.add(vvod.getId());
                cntThreads=1;
            } else if (uk != null) {
                // по УК
                lstItems = vvodDAO.findVvodByUk(uk.getReu())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            } else if (house != null) {
                // по дому
                lstItems = vvodDAO.findVvodByHouse(house.getId())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads=1;
            } else {
                // все вводы
                isLockForLongLastingProcess = true;
                lstItems = vvodDAO.findVvodAll()
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            }
        }
    }

    /**
     * Подготовка хранилища для расчета ОДН
     */
    public void prepareChrgCountAmount() {
        // хранилище параметров по дому (для ОДН и прочих нужд)
        setChrgCountAmount(new ChrgCountAmount());
    }

    /**
     * Построить CalcStore
     *
     * @param genDt - дата формирования
     * @param tp    - тип операции
     * @return
     */
    private CalcStore buildCalcStore(Date genDt, int tp) {
        ConfigApp config = SpringContext.getBean(ConfigApp.class);
        PenDtDAO penDtDAO = SpringContext.getBean(PenDtDAO.class);
        PenRefDAO penRefDAO = SpringContext.getBean(PenRefDAO.class);
        CalcStore calcStore = new CalcStore();
        // дата начала периода
        calcStore.setCurDt1(config.getCurDt1());
        // дата окончания периода
        calcStore.setCurDt2(config.getCurDt2());
        // дата расчета пени
        calcStore.setGenDt(genDt);
        // текущий период
        calcStore.setPeriod(Integer.valueOf(config.getPeriod()));
        // период - месяц назад
        calcStore.setPeriodBack(Integer.valueOf(config.getPeriodBack()));
        log.info("Начало получения справочников");
        if (tp == 1) {
            // справочник дат начала пени
            calcStore.setLstPenDt(penDtDAO.findAll());
            log.info("Загружен справочник дат начала обязательства по оплате");
            // справочник ставок рефинансирования
            calcStore.setLstPenRef(penRefDAO.findAll());
            log.info("Загружен справочник ставок рефинансирования");
        }
        return calcStore;
    }

    // получить следующий id, для расчета в потоках
    public synchronized Long getNextItem() {
        Iterator<Long> itr = lstItems.iterator();
        Long item = null;
        if (itr.hasNext()) {
            item = itr.next();
            itr.remove();
        }
        return item;
    }

    public static final class RequestConfigDirectBuilder {
        // Id запроса
        int rqn;
        // тип выполнения 0-начисление, 3-начисление для распределения по вводу, 1-задолженность и пеня, 2 - распределение объемов по вводу,
        // 4 - начисление по одной услуге, для автоначисления
        int tp = 0;
        // уровень отладки
        int debugLvl = 0;
        // дата на которую сформировать
        Date genDt = null;
        // текущий период
        Date curDt1;
        Date curDt2;
        // выполнять многопоточно
        boolean isMultiThreads = false;
        // объекты формирования:
        // УК
        Org uk = null;
        // дом
        House house = null;
        // ввод
        Vvod vvod = null;
        // помещение
        Ko ko = null;
        // услуга
        Usl usl = null;
        // список Id объектов формирования
        List<Long> lstItems;
        // хранилище справочников
        CalcStore calcStore;
        // выбранный тип объекта формирования
        private int tpSel;
        // хранилище объемов по вводу (дому) (для ОДН и прочих нужд)
        private ChrgCountAmount chrgCountAmount;

        private RequestConfigDirectBuilder() {
        }

        public static RequestConfigDirectBuilder aRequestConfigDirect() {
            return new RequestConfigDirectBuilder();
        }

        public RequestConfigDirectBuilder withRqn(int rqn) {
            this.rqn = rqn;
            return this;
        }

        public RequestConfigDirectBuilder withTp(int tp) {
            this.tp = tp;
            return this;
        }

        public RequestConfigDirectBuilder withDebugLvl(int debugLvl) {
            this.debugLvl = debugLvl;
            return this;
        }

        public RequestConfigDirectBuilder withGenDt(Date genDt) {
            this.genDt = genDt;
            return this;
        }

        public RequestConfigDirectBuilder withCurDt1(Date curDt1) {
            this.curDt1 = curDt1;
            return this;
        }

        public RequestConfigDirectBuilder withCurDt2(Date curDt2) {
            this.curDt2 = curDt2;
            return this;
        }

        public RequestConfigDirectBuilder withIsMultiThreads(boolean isMultiThreads) {
            this.isMultiThreads = isMultiThreads;
            return this;
        }

        public RequestConfigDirectBuilder withUk(Org uk) {
            this.uk = uk;
            return this;
        }

        public RequestConfigDirectBuilder withHouse(House house) {
            this.house = house;
            return this;
        }

        public RequestConfigDirectBuilder withVvod(Vvod vvod) {
            this.vvod = vvod;
            return this;
        }

        public RequestConfigDirectBuilder withKo(Ko ko) {
            this.ko = ko;
            return this;
        }

        public RequestConfigDirectBuilder withUsl(Usl usl) {
            this.usl = usl;
            return this;
        }

        // строить не builder-ом!!!
        public RequestConfigDirect build() {
            RequestConfigDirect requestConfigDirect = new RequestConfigDirect();
            requestConfigDirect.setRqn(rqn);
            requestConfigDirect.setTp(tp);
            requestConfigDirect.setDebugLvl(debugLvl);
            requestConfigDirect.setGenDt(genDt);
            requestConfigDirect.setCurDt1(curDt1);
            requestConfigDirect.setCurDt2(curDt2);
            requestConfigDirect.setUk(uk);
            requestConfigDirect.setHouse(house);
            requestConfigDirect.setVvod(vvod);
            requestConfigDirect.setKo(ko);
            requestConfigDirect.setTpSel(tpSel);
            requestConfigDirect.setUsl(usl);
            requestConfigDirect.setLstItems(lstItems);
            requestConfigDirect.setCalcStore(calcStore);
            requestConfigDirect.isMultiThreads = this.isMultiThreads;

            requestConfigDirect.setCalcStore(requestConfigDirect.buildCalcStore(genDt, tp));
            return requestConfigDirect;
        }
    }
}
