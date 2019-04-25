package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.dao.HouseDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.SprGenItm;
import com.dic.bill.model.scott.Stub;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Сервис итогового формирования
 *
 * @version 1.0
 */
@Slf4j
@Service
@Scope("prototype")
public class GenMainMngImpl implements GenMainMng, CommonConstants {

    private final SprParamMng sprParamMng;
    private final ConfigApp config;
    private final SprGenItmDAO sprGenItmDao;
    private final HouseDAO houseDao;
    private final ExecMng execMng;
    private final MntBase mntBase;
    private final ApplicationContext ctx;
    private final ThreadMng<Long> threadMng;
    private final WebController webController;
    @PersistenceContext
    private EntityManager em;

    public GenMainMngImpl(ConfigApp config, SprGenItmDAO sprGenItmDao, HouseDAO houseDao, ExecMng execMng, MntBase mntBase, ApplicationContext ctx, ThreadMng<Long> threadMng, WebController webController, SprParamMng sprParamMng) {
        this.config = config;
        this.sprGenItmDao = sprGenItmDao;
        this.houseDao = houseDao;
        this.execMng = execMng;
        this.mntBase = mntBase;
        this.ctx = ctx;
        this.threadMng = threadMng;
        this.webController = webController;
        this.sprParamMng = sprParamMng;
    }

    /**
     * ОСНОВНОЙ поток формирования
     */
    @Override
    @Async
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void startMainThread() {
        // маркер итогового формирования
        config.getLock().setLockProc(1, "AmountGeneration");
        SprGenItm menuGenItg = sprGenItmDao.getByCd("GEN_ITG");
        execMng.setMenuElemState(menuGenItg, null);
        execMng.setMenuElemDt1(menuGenItg, new Date());
        execMng.setMenuElemDt2(menuGenItg, null);

        //sprGenItmDao.getAllOrdered().stream().forEach(t->execMng.set);

        try {
            // прогресс - 0
            config.setProgress(0);

            SprGenItm menuMonthOver = sprGenItmDao.getByCd("GEN_MONTH_OVER");
            SprGenItm menuCheckBG = sprGenItmDao.getByCd("GEN_CHECK_BEFORE_GEN");
            SprGenItm menuCheckAG = sprGenItmDao.getByCd("GEN_CHECK_AFTER_GEN");

            //********** почистить ошибку последнего формирования, % выполнения
            //genMng.clearError(menuGenItg);

            //********** установить дату формирования
            execMng.setGenDate();

            //**********Закрыть базу для пользователей, если выбрано итоговое формир
            if (menuGenItg.getSel()) {
                execMng.stateBase(1);
                log.info("Установлен признак закрытия базы!");
            }

            //********** Проверки до формирования
            if (menuCheckBG.getSel()) {
                // если выбраны проверки, а они как правило д.б. выбраны при итоговом
                if (checkErrBeforeGen(menuCheckBG)) {
                    // найдены ошибки - выход
                    menuGenItg.setState("Найдены ошибки до формирования!");
                    log.info("Найдены ошибки до формирования!");
                    return;
                }
                if (markExecuted(menuGenItg, menuCheckBG, 0.05D, new Date())) return;
                log.info("Проверки до формирования выполнены!");
            }

            //********** Проверки до перехода месяца note Переделать этот бред!
/*            if (menuMonthOver.getSel()) {
                if (checkMonthOverErr()) {
                    // найдены ошибки - выход
                    menuGenItg.setState("Найдены ошибки до перехода месяца!");
                    log.info("Найдены ошибки до перехода месяца!");
                    return;
                }
                execMng.setMenuElemPercent(menuMonthOver, 1);
                log.info("Проверки до перехода месяца выполнены!");
            }
            execMng.setMenuElemPercent(menuGenItg, 0.10D);
*/
            // список Id объектов
            List<Long> lst;
            String retStatus;
            //********** Начать формирование
            for (SprGenItm itm : sprGenItmDao.getAllCheckedOrdered()) {

                log.info("Generating menu item: {}", itm.getCd());
                Integer ret;
                Date dt1;
                switch (itm.getCd()) {
                    case "GEN_ADVANCE":
                        // переформировать авансовые платежи
                        dt1 = new Date();
                        execMng.execProc(36, null, null);
                        if (markExecuted(menuGenItg, itm, 0.20D, dt1)) return;
                        break;
                    case "GEN_DIST_VOLS4":
                        // распределение объемов в Java
                        dt1 = new Date();
                        retStatus = webController.gen(2, 0, 0L, 0L, 0, null, null,
                                Utl.getStrFromDate(config.getCurDt2()), 0);
                        if (!retStatus.substring(0, 2).equals("OK")) {
                            // ошибка начисления
                            execMng.setMenuElemState(menuGenItg, "Ошибка во время распределения объемов!");
                            log.error("Ошибка во время распределения объемов!");
                            return;
                        }
                        if (markExecuted(menuGenItg, itm, 0.25D, dt1)) return;
                        break;
                    case "GEN_CHRG":
                        // начисление по всем помещениям в Java
                        dt1 = new Date();
                        retStatus = webController.gen(0, 0, 0L, 0L, 0, null, null,
                                Utl.getStrFromDate(config.getCurDt2()), 0);
                        if (!retStatus.substring(0, 2).equals("OK")) {
                            // ошибка начисления
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки во время расчета начисления!");
                            log.error("Найдены ошибки во время расчета начисления!");
                            return;
                        }
                        if (markExecuted(menuGenItg, itm, 0.40D, dt1)) return;
                        break;
                    case "GEN_SAL":
                        //сальдо по лиц счетам
                        dt1 = new Date();
                        execMng.execProc(19, null, null);
                        if (markExecuted(menuGenItg, itm, 0.25D, dt1)) return;
                        break;
                    case "GEN_FLOW":
                        // движение
                        dt1 = new Date();
                        execMng.execProc(20, null, null);
                        if (markExecuted(menuGenItg, itm, 0.50D, dt1)) return;
                        break;
                    case "GEN_PENYA":
                        // начисление пени по домам
                        dt1 = new Date();
                        lst = houseDao.findAll()
                                .stream().map(t -> t.getId().longValue()).collect(Collectors.toList());
                        if (!doInThread(lst, itm)) {
                            // ошибка распределения
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки во время начисления пени по домам!");
                            log.error("Найдены ошибки во время начисления пени по домам!");
                            return;
                        }
                        if (markExecuted(menuGenItg, itm, 0.55D, dt1)) return;
                        break;
                    case "GEN_PENYA_DIST":
                        // распределение пени по исх сальдо
                        dt1 = new Date();
                        execMng.execProc(21, null, null);
                        if (markExecuted(menuGenItg, itm, 0.60D, dt1)) return;
                        break;
                    case "GEN_SAL_HOUSES":
                        // оборотная ведомость по домам
                        dt1 = new Date();
                        execMng.execProc(22, null, null);
                        if (markExecuted(menuGenItg, itm, 0.65D, dt1)) return;
                        break;
                    case "GEN_XITO14":
                        // начисление по услугам (надо ли оно кому???)
                        dt1 = new Date();
                        execMng.execProc(23, null, null);
                        if (markExecuted(menuGenItg, itm, 0.70D, dt1)) return;
                        break;
                    case "GEN_F3_1":
                        // оплата по операциям
                        dt1 = new Date();
                        execMng.execProc(24, null, null);
                        if (markExecuted(menuGenItg, itm, 0.75D, dt1)) return;
                        break;
                    case "GEN_F3_1_2":
                        // оплата по операциям, для оборотной
                        dt1 = new Date();
                        execMng.execProc(25, null, null);
                        if (markExecuted(menuGenItg, itm, 0.77D, dt1)) return;
                        break;
                    case "GEN_F2_4":
                        // по УК-организациям Ф.2.4.
                        dt1 = new Date();
                        execMng.execProc(26, null, null);
                        if (markExecuted(menuGenItg, itm, 0.78D, dt1)) return;
                        break;
                    case "GEN_F1_1":
                        // по пунктам начисления
                        dt1 = new Date();
                        execMng.execProc(27, null, null);
                        if (markExecuted(menuGenItg, itm, 0.79D, dt1)) return;
                        break;
                    case "GEN_ARCH_BILLS":
                        // архив, счета
                        dt1 = new Date();
                        execMng.execProc(28, null, null);
                        if (markExecuted(menuGenItg, itm, 0.80D, dt1)) return;
                        break;
                    case "GEN_DEBTS":
                        // задолжники
                        dt1 = new Date();
                        execMng.execProc(29, null, null);
                        if (markExecuted(menuGenItg, itm, 0.83D, dt1)) return;
                        break;
                    case "GEN_EXP_LISTS":
                        // списки
                        dt1 = new Date();
                        execMng.execProc(30, null, null);
                        execMng.execProc(31, null, null);
                        execMng.execProc(32, null, null);
                        if (markExecuted(menuGenItg, itm, 0.85D, dt1)) return;
                        break;
                    case "GEN_STAT":
                        // статистика
                        dt1 = new Date();
                        execMng.execProc(33, null, null);
                        if (markExecuted(menuGenItg, itm, 0.90D, dt1)) return;
                        break;
                    case "GEN_COMPRESS_ARCH":
                        // сжатие архивов
                        dt1 = new Date();
                        if (!mntBase.comprAllTables("00000000", null, "anabor", false)) {
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки при сжатии таблицы a_nabor2!");
                            log.error("Найдены ошибки при сжатии таблицы a_nabor2!");
                            // выйти при ошибке
                            return;
                        }
                        if (markExecuted(menuGenItg, menuGenItg, 0.20D, dt1)) return;
                        execMng.setMenuElemPercent(itm, 0.92);
                        if (!mntBase.comprAllTables("00000000", null, "acharge", false)) {
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки при сжатии таблицы a_charge2!");
                            log.error("Найдены ошибки при сжатии таблицы a_charge2!");
                            // выйти при ошибке
                            return;
                        }
                        if (markExecuted(menuGenItg, menuGenItg, 0.20D, dt1)) return;
                        execMng.setMenuElemPercent(itm, 0.95);
                        if (!mntBase.comprAllTables("00000000", null, "achargeprep", false)) {
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки при сжатии таблицы a_charge_prep2!");
                            log.error("Найдены ошибки при сжатии таблицы a_charge_prep2!");
                            // выйти при ошибке
                            return;
                        }
                        setMenuProc(menuGenItg, itm, 0.96D, dt1, new Date());
                        if (markExecuted(menuGenItg, itm, 1D, dt1)) return;
                        break;
                }
            }

            //********** Проверки после формирования
            if (menuCheckAG.getSel()) {
                // если выбраны проверки, а они как правило д.б. выбраны при итоговом
                if (checkErrAfterGen(menuCheckAG)) {
                    // найдены ошибки - выход
                    menuGenItg.setState("Найдены ошибки после формирования!");
                    log.info("Найдены ошибки до формирования!");
                    return;
                }
                if (markExecuted(menuGenItg, menuCheckAG, 0.99D, new Date())) return;
                log.info("Проверки после формирования выполнены!");
            }

            // выполнено всё
            if (menuGenItg.getSel()) {
                execMng.setMenuElemState(menuGenItg, "Выполнено успешно!");
                execMng.setMenuElemPercent(menuGenItg, 1D);
            }
        } catch (Exception e) {
            log.error(Utl.getStackTraceString(e));
            execMng.setMenuElemState(menuGenItg, "Ошибка! Смотреть логи! ".concat(e.getMessage()));
            // прогресс формирования +1, чтоб отобразить ошибку на web странице
            config.incProgress();

        } finally {
            // формирование остановлено
            // снять маркер выполнения
            config.getLock().unlockProc(1, "AmountGeneration");
            execMng.setMenuElemDt2(menuGenItg, new Date());
            // прогресс формирования +1, чтоб отобразить статус на web странице
            config.incProgress();
        }
    }

    /**
     * Маркировать выполненным
     *
     * @param genItg - элемент Итогового формирования
     * @param itm    - текущий элемент
     * @param proc   - % выполнения
     * @param dt1    - дата-время начала
     * @return - формирование остановлено?
     */
    private boolean markExecuted(SprGenItm genItg, SprGenItm itm, double proc, Date dt1) {
        setMenuProc(genItg, itm, proc, dt1, new Date());
        if (config.getLock().isStopped(stopMarkAmntGen)) {
            execMng.setMenuElemState(genItg, "Остановлено!");
            execMng.setMenuElemState(itm, "Остановлено!");
            log.error("Остановлено!");
            return true;
        }
        return false;
    }

    private void setMenuProc(SprGenItm menuGenItg, SprGenItm itm, Double proc, Date dt1, Date dt2) {
        execMng.setMenuElemPercent(itm, 1);
        execMng.setMenuElemDt1(itm, dt1);
        execMng.setMenuElemDt2(itm, dt2);
        execMng.setMenuElemState(itm, "Выполнено успешно");
        if (menuGenItg.getSel()) {
            execMng.setMenuElemPercent(menuGenItg, proc);
        }
        // прогресс формирования +1
        config.incProgress();
    }

    /**
     * Выполнение в потоках
     *
     * @param lst - список Id вводов
     * @param spr - элемент меню
     */
    private boolean doInThread(List<Long> lst, SprGenItm spr) {
        // будет выполнено позже, в создании потока
        PrepThread<Long> reverse = (item, proc) -> {
            // сохранить процент выполнения
            //execMng.setPercent(spr, proc);
            // потоковый сервис
            GenThrMng genThrMng = ctx.getBean(GenThrMng.class);
            return genThrMng.doJob(3, item, spr, proc);
        };

        // вызвать в потоках
        try {
            // вызвать потоки, проверять наличие маркера работы процесса
            threadMng.invokeThreads(reverse, 5, lst, true, 0, "AmountGeneration");
        } catch (InterruptedException | ExecutionException | WrongParam | ErrorWhileChrg | ErrorWhileGen e) {
            log.error(Utl.getStackTraceString(e));
            return false;
        }
        return true;
    }

    /**
     * Проверка ошибок до формирования
     * вернуть false - если нет ошибок
     *
     * @param menuCheckBG - строка меню
     */
    private boolean checkErrBeforeGen(SprGenItm menuCheckBG) throws WrongParam {
        if (checkErrVar(menuCheckBG, Kart.class, 1, "ВНИМАНИЕ! Лицевые, содержащие некорректную " +
                "дату прописки-выписки в проживающих:"))
            return true;
        if (checkErrVar(menuCheckBG, Kart.class, 2, "ВНИМАНИЕ! Лицевые, содержащие пересекающийся " +
                "период статуса прописки:"))
            return true;
        if (checkErrVar(menuCheckBG, Kart.class, 3, "ВНИМАНИЕ! Лицевые, содержащие пересекающийся " +
                "период статуса прописки:"))
            return true;
        if (checkErrVar(menuCheckBG, Kart.class, 4, "ВНИМАНИЕ! Лицевые, содержащие некорректное кол-во " +
                "проживающих в карточке:"))
            return true;
        // проверка показаний счетчиков
        if (Utl.nvl(sprParamMng.getN1("CONTROL_METER"), 1D).equals(1D)) {
            if (checkErrVar(menuCheckBG, Kart.class, 5, "ВНИМАНИЕ! Лицевые, содержащие некорректные " +
                    "показания счетчиков:"))
                return true;
        }
        if (checkErrVar(menuCheckBG, Stub.class, 6, "ВНИМАНИЕ! Не идёт общая сумма " +
                "в C_KWTP и C_KWTP_MG:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 7, "ВНИМАНИЕ! Не проинкассированы " +
                "следующие компьютеры:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 8, "ВНИМАНИЕ! Изменилось кол-во полей! " +
                "Необходимо исправить модель в Java: A_CHARGE_PREP2"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 9, "ВНИМАНИЕ! Изменилось кол-во полей! " +
                "Необходимо исправить модель в Java: A_CHARGE2"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 10, "ВНИМАНИЕ! Изменилось кол-во полей! " +
                "Необходимо исправить модель в Java: A_NABOR2"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 11, "ВНИМАНИЕ! Не идёт общая сумма " +
                "в C_KWTP и KWTP_DAY:"))
            return true;

        return false;
    }

    /**
     * Проверка ошибок после формирования
     * вернуть false - если нет ошибок
     *
     * @param menuCheckBG - строка меню
     */
    private boolean checkErrAfterGen(SprGenItm menuCheckBG) throws WrongParam {
        if (checkErrVar(menuCheckBG, Stub.class, 100, "ВНИМАНИЕ! Лицевые содержат некорректное " +
                "распределение пени (T_CHPENYA_FOR_SALDO):"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 101, "ВНИМАНИЕ! Лицевые содержат некорректное " +
                "исх. сальдо по пене (A_PENYA):"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 102, "ВНИМАНИЕ! Не идет исходящее сальдо SALDO_USL и XITOG3_LSK " +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 103, "ВНИМАНИЕ! Не идет исходящий долг в C_CHARGEPAY и A_PENYA " +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 104, "ВНИМАНИЕ! Не идет начисление в оборотке XITOG3_LSK и C_CHARGE" +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 105, "ВНИМАНИЕ! Не идут перерасчеты в оборотке XITOG3_LSK и C_CHANGE" +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 106, "ВНИМАНИЕ! Не идет оплата в оборотке XITOG3_LSK и KWTP_DAY " +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 107, "ВНИМАНИЕ! Не идет сальдо с движением C_CHARGEPAY и SALDO_USL "))
            return true;
        if (checkErrVar(menuCheckBG, Kart.class, 108, "ВНИМАНИЕ! Не идет сальдо с движением C_CHARGEPAY и SALDO_USL " +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 109, "ВНИМАНИЕ! В сальдо SALDO_USL присутствуют организации ниже уровня 1: "))
            return true;
        if (Utl.nvl(sprParamMng.getN1("HAVE_LK"), 1D).equals(1D)) {
            if (checkErrVar(menuCheckBG, Stub.class, 110, "ВНИМАНИЕ! Не все показания счетчиков из личного " +
                    "кабинета были учтены в базе!"))
                return true;
            if (checkErrVar(menuCheckBG, Stub.class, 111, "ВНИМАНИЕ! Несоответствующий период в личном кабинете!!"))
                return true;
        }
        if (checkErrVar(menuCheckBG, Stub.class, 112, "ВНИМАНИЕ! В оборотке XITOG3_LSK некорректно сальдо по пене " +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 113, "ВНИМАНИЕ!  Не идет пеня в оборотке XITOG3_LSK и A_PENYA " +
                "по следующим лиц.:"))
            return true;
        if (checkErrVar(menuCheckBG, Stub.class, 114, "ВНИМАНИЕ!  Не идет пеня в T_CHPENYA_FOR_SALDO и C_PENYA " +
                "по следующим лиц.:"))
            return true;

        return false;
    }

    /**
     * Проверка ошибок
     * вернуть false - если нет ошибок
     *
     * @param menuCheckBG - строка меню
     * @param t           - тип возвращаемых объектов из REFCURSOR
     * @param var         - вариант проверки в процедуре SCOTT.P_THREAD.EXTENDED_CHK
     */
    private <T> boolean checkErrVar(SprGenItm menuCheckBG, Class<T> t, int var, String strMes) throws WrongParam {
        log.info("Выполняется проверка var={}", var);
        StoredProcedureQuery procedureQuery;
        procedureQuery =
                em.createStoredProcedureQuery("SCOTT.P_THREAD.EXTENDED_CHK", t);
        procedureQuery.registerStoredProcedureParameter("P_VAR", Integer.class, ParameterMode.IN);
        procedureQuery.registerStoredProcedureParameter("PREP_REFCURSOR", void.class, ParameterMode.REF_CURSOR);
        procedureQuery.setParameter("P_VAR", var);
        procedureQuery.execute();

        String strErr = null;
        switch (t.getName()) {
            case "com.dic.bill.model.scott.Kart": {
                @SuppressWarnings("unchecked")
                List<Kart> lst = procedureQuery.getResultList();
                strErr = printStrKart(lst);
                break;
            }
            case "com.dic.bill.model.scott.House": {
                @SuppressWarnings("unchecked")
                List<House> lst = procedureQuery.getResultList();
                strErr = printStrHouse(lst);
                break;
            }
            case "com.dic.bill.model.scott.Stub": {
                @SuppressWarnings("unchecked")
                List<Stub> lst = procedureQuery.getResultList();
                strErr = printStrStub(lst);
                break;
            }
            default: {
                throw new WrongParam("Некорректный параметр var=" + var);
            }
        }

        if (strErr != null && strErr.length() > 0) {
            // ошибки
            menuCheckBG.setState("Var="+ var+" "+strMes + strErr);
            return true;
        } else {
            // нет ошибок
            return false;
        }
    }

    private String printStrKart(List<Kart> lst) {
        return lst.stream().map(Kart::getLsk).limit(100)
                .collect(Collectors.joining(","));
    }

    private String printStrHouse(List<House> lst) {
        return lst.stream().map(t -> String.valueOf(t.getId())).limit(100)
                .collect(Collectors.joining(","));
    }

    private String printStrStub(List<Stub> lst) {
        return lst.stream().map(Stub::getText).limit(100)
                .collect(Collectors.joining(","));
    }

    /**
     * Проверка ошибок до перехода
     * вернуть false - если нет ошибок
     */
    private boolean checkMonthOverErr() {
        // новая проверка, на список домов в разных УК, по которым обнаружены открытые лицевые счета
        Integer ret = execMng.execProc(8, null, null);
        boolean isErr = false;
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(10, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(11, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(14, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }

        return isErr;
    }

}