package com.dic.app.mm.impl;

import com.dic.app.mm.*;
import com.dic.bill.dao.HouseDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.SprGenItm;
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

    public GenMainMngImpl(ConfigApp config, SprGenItmDAO sprGenItmDao, HouseDAO houseDao, ExecMng execMng, MntBase mntBase, ApplicationContext ctx, ThreadMng<Long> threadMng, WebController webController) {
        this.config = config;
        this.sprGenItmDao = sprGenItmDao;
        this.houseDao = houseDao;
        this.execMng = execMng;
        this.mntBase = mntBase;
        this.ctx = ctx;
        this.threadMng = threadMng;
        this.webController = webController;
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
                if (checkErr(menuCheckBG)) {
                    // найдены ошибки - выход
                    menuGenItg.setState("Найдены ошибки до формирования!");
                    log.info("Найдены ошибки до формирования!");
                    return;
                }
                execMng.setMenuElemPercent(menuCheckBG, 1);
                log.info("Проверки до формирования выполнены!");
            }

            //********** Проверки до перехода месяца
            if (menuMonthOver.getSel()) {
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
/*                        lst = houseDao.getNotClosed()
                                .stream().map(t -> t.getId().longValue()).collect(Collectors.toList()); ред.07.04.2019 - убрал фильтр закрытых - не просчитывается пеня!*/
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
                        // проверить распр.пени
                        ret = execMng.execProc(13, null, null);
                        if (ret.equals(1)) {
                            // найдены ошибки - выход
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки в процессе проверки распределения пени!");
                            log.error("Найдены ошибки в процессе проверки распределения пени!");
                            return;
                        }
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
                        // проверить распр.пени, после того как она переписана в архив
                        ret = execMng.execProc(37, null, null);
                        if (ret.equals(1)) {
                            // найдены ошибки - выход
                            execMng.setMenuElemState(menuGenItg, "Найдены ошибки в распр.пени, после того как она переписана в архив!");
                            log.error("Найдены ошибки в распр.пени, после того как она переписана в архив!");
                            return;
                        }
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
                        setMenuProc(menuGenItg, itm, 0.99D, dt1, new Date());
                        if (markExecuted(menuGenItg, itm, 1D, dt1)) return;
                        break;
                }
            }

            // выполнено всё
            execMng.setMenuElemPercent(menuGenItg, 1D);
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
        execMng.setMenuElemPercent(menuGenItg, proc);
        // прогресс формирования +1
        config.incProgress();
    }

    /**
     * Выполнение в потоках
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
     * Проверка ошибок
     * вернуть false - если нет ошибок
     * @param menuCheckBG - строка меню
     */
    private boolean checkErr(SprGenItm menuCheckBG) {
        StoredProcedureQuery procedureQuery =
                em.createStoredProcedureQuery("scott.p_thread.extended_chk", Kart.class);
        procedureQuery.registerStoredProcedureParameter("p_var", Integer.class, ParameterMode.IN);
        procedureQuery.registerStoredProcedureParameter("prep_refcursor", void.class, ParameterMode.REF_CURSOR);
        procedureQuery.setParameter("p_var", 1);
        procedureQuery.execute();

        @SuppressWarnings("unchecked")
        List<Kart> lstKart = procedureQuery.getResultList();
        lstKart.forEach(t-> log.info("ПРОВЕРКА! lsk={}", t.getLsk()));
        String strErr = lstKart.stream().map(Kart::getLsk).limit(100)
                .collect(Collectors.joining(","));

        if (strErr != null) {
            // ошибки
            menuCheckBG.setState("По следующим лиц.сч. некорректно кол-во проживающих:"+strErr);
            return true;
        } else {
            // нет ошибок
            return false;
        }

        // новая проверка, на список домов в разных УК, по которым обнаружены открытые лицевые счета
        /*Integer ret = execMng.execProc(38, null, null);
        boolean isErr = false;
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(4, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(5, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(6, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
        ret = execMng.execProc(7, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }

        ret = execMng.execProc(12, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }

        ret = execMng.execProc(8, null, null);
        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }

        ret = execMng.execProc(15, null, null);

        if (ret.equals(1)) {
            // установить статус ошибки, выйти из формирования
            isErr = true;
        }
         */

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