package com.dic.app.mm.impl;

import com.dic.app.mm.ExecMng;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.SprGenItm;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class ExecMngImpl implements ExecMng {

    @PersistenceContext
    private EntityManager em;
    private final SprGenItmDAO sprGenItmDao;

    public ExecMngImpl(SprGenItmDAO sprGenItmDao) {
        this.sprGenItmDao = sprGenItmDao;
    }

    /**
     * Обновить элемент меню
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateSprGenItem(List<SprGenItm> lst) {
        lst.forEach(t -> {
            SprGenItm sprGenItm = em.find(SprGenItm.class, t.getId());
            if (t.getSel() != null) {
                sprGenItm.setSel(t.getSel());
            }
        });
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Integer execProc(Integer var, Long id, Integer sel) {
        StoredProcedureQuery qr;
        Integer ret = null;

        switch (var) {
            // проверки ошибок (оставил несколько проверок здесь - после распределения пени и после архивов)
            case 13:
            case 37:
                qr = em.createStoredProcedureQuery("scott.gen.gen_check");
                qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.OUT);
                qr.registerStoredProcedureParameter(2, String.class, ParameterMode.OUT);
                qr.registerStoredProcedureParameter(3, Integer.class, ParameterMode.IN);
                // перекодировать в gen.gen_check код выполнения
                int par = 0;
                switch (var) {
                    case 13:
                        par=6;
                        break;
                    case 37:
                        par=9;
                        break;
                }
                qr.setParameter(3, par);
                qr.execute();
                ret = (Integer) qr.getOutputParameterValue(1);
                log.info("Проверка ошибок scott.gen.gen_check с параметром var_={}, дала результат err_={}", par, ret);
        // проверки ошибок
		/*case 4:
		case 5:
		case 6:
		case 7:
		case 38:
			qr = em.createStoredProcedureQuery("scott.p_thread.smpl_chk");
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
			qr.registerStoredProcedureParameter(2, Integer.class, ParameterMode.OUT);
			// перекодировать в gen.smpl_chck код выполнения
			switch (var) {
			case 4:
				par=1;
				break;
			case 5:
				par=2;
				break;
			case 6:
				par=3;
				break;
			case 7:
				par=4;
				break;
			case 38:
				par=5;
				break;
			}
			qr.setParameter(1, par);
			qr.execute();
			ret = (Integer) qr.getOutputParameterValue(2);
			log.info("Проверка ошибок scott.p_thread.smpl_chk с параметром var_={}, дала результат err_={}", par, ret);
			break;
		// проверки ошибок
		case 8:
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
		case 37:
			qr = em.createStoredProcedureQuery("scott.gen.gen_check");
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.OUT);
			qr.registerStoredProcedureParameter(2, String.class, ParameterMode.OUT);
			qr.registerStoredProcedureParameter(3, Integer.class, ParameterMode.IN);

			// перекодировать в gen.gen_check код выполнения
			switch (var) {
			case 8:
				par=1;
				break;
			case 9:
				par=2;
				break;
			case 10:
				par=3;
				break;
			case 11:
				par=4;
				break;
			case 12:
				par=5;
				break;
			case 13:
				par=6;
				break;
			case 14:
				par=7;
				break;
			case 15:
				par=8;
				break;
			case 37:
				par=9;
				break;
			}
			qr.setParameter(3, par);
			qr.execute();
			ret = (Integer) qr.getOutputParameterValue(1);
			log.info("Проверка ошибок scott.gen.gen_check с параметром var_={}, дала результат err_={}", par, ret);
			break;*/
            case 16:
                // установить текущую дату, до формирования
                qr = em.createStoredProcedureQuery("scott.init.set_date_for_gen");
                qr.executeUpdate();
                break;
            case 17:
                // чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
                qr = em.createStoredProcedureQuery("scott.p_thread.gen_clear_vol");
                qr.executeUpdate();
                break;
            case 19:
                // сальдо по лиц счетам
                qr = em.createStoredProcedureQuery("scott.gen.gen_saldo");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 20:
                // движение
                qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_full");
                qr.executeUpdate();
                break;
            case 21:
                // распределение пени по исх сальдо
                qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_pen");

                qr.registerStoredProcedureParameter(1, Date.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, Long.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.setParameter(2, 0L);
                qr.executeUpdate();
                break;
            case 22:
                // сальдо по домам
                qr = em.createStoredProcedureQuery("scott.gen.gen_saldo_houses");
                qr.executeUpdate();
                break;
            case 23:
                // начисление по услугам (надо ли оно кому???)
                qr = em.createStoredProcedureQuery("scott.gen.gen_xito13");
                qr.executeUpdate();
                break;
            case 24:
                // оплата по операциям Ф.3.1.
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito5");
                qr.executeUpdate();
                break;
            case 25:
                // оплата по операциям Ф.3.1. для оборотки
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito5_");
                qr.executeUpdate();
                break;
            case 26:
                // по УК-организациям Ф.2.4.
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito10");
                qr.executeUpdate();
                break;
            case 27:
                // по пунктам начисления
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito3");
                qr.executeUpdate();
                break;
            case 28:
                // архив, счета
                qr = em.createStoredProcedureQuery("scott.gen.prepare_arch");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 29:
                // задолжники
                qr = em.createStoredProcedureQuery("scott.gen.gen_debits_lsk_month");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 31:
                // cписки - changes
                qr = em.createStoredProcedureQuery("scott.c_exp_list.changes_export");
                qr.executeUpdate();
                break;
            case 32:
                // cписки - charges
                qr = em.createStoredProcedureQuery("scott.c_exp_list.charges_export");
                qr.executeUpdate();
                break;
            case 33:
                // cтатистика
                qr = em.createStoredProcedureQuery("scott.gen_stat.gen_stat_usl");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 35:
                // вызов из WebCtrl
                qr = em.createStoredProcedureQuery("scott.p_thread.check_itms");
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, Integer.class, ParameterMode.IN);

                qr.setParameter(1, id);
                qr.setParameter(2, sel);
                qr.executeUpdate();
                break;
            case 36:
                // перераспределение авансовых платежей
                qr = em.createStoredProcedureQuery("scott.c_dist_pay.dist_pay_lsk_avnc_force");
                qr.executeUpdate();
                break;
            case 100:
                // распределить ОДН во вводах, где нет ОДПУ
                qr = em.createStoredProcedureQuery("scott.p_vvod.gen_dist_wo_vvod_usl");
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;
            case 101:
                // распределить ОДН во вводах, где есть ОДПУ
                qr = em.createStoredProcedureQuery("scott.p_thread.gen_dist_odpu");
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;
            case 102:
                // начислить пеню по домам
                qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_pen_house");
                // дата, не заполняем, null
                qr.registerStoredProcedureParameter(1, Date.class, ParameterMode.IN);
                // id дома
                qr.registerStoredProcedureParameter(2, Long.class, ParameterMode.IN);
                qr.setParameter(1, new Date());
                qr.setParameter(2, id);
                qr.executeUpdate();
                break;
            case 103:
                // расчитать начисление по домам
                qr = em.createStoredProcedureQuery("scott.c_charges.gen_charges");
                // id дома
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;

            default:
                break;
        }

        return ret;
    }

    /**
     * Очистить ошибку формирования
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void clearError(SprGenItm menuGenItg) {
        //почистить % выполнения
        for (SprGenItm itm : sprGenItmDao.findAll()) {
            itm.setProc((double) 0);
            itm.setState(null);
            itm.setDt1(null);
            itm.setDt2(null);
        }
        menuGenItg.setState(null);
        menuGenItg.setErr(0);
        menuGenItg.setState(null);
        menuGenItg.setDt1(new Date());
    }

    /**
     * Установить дату формирования
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void setGenDate() {
        execProc(16, null, null);
    }

    /**
     * Закрыть или открыть базу для пользователей
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void stateBase(int state) {
        execProc(3, null, state);
    }

    /**
     * Установить процент выполнения в элементе меню
     *
     * @param spr  - элемент меню
     * @param proc - процент
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemPercent(SprGenItm spr, double proc) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setProc(proc);
    }

    /**
     * Установить строку состояния в элементе меню
     *
     * @param spr   - элемент меню
     * @param state - строка
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemState(SprGenItm spr, String state) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setState(state);
    }

    /**
     * Установить дату начала формирования в элементе меню
     *
     * @param spr  - элемент меню
     * @param dt1- дата начала формирования
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemDt1(SprGenItm spr, Date dt1) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setDt1(dt1);
    }

    /**
     * Установить окончания начала формирования в элементе меню
     *
     * @param spr  - элемент меню
     * @param dt2- дата начала формирования
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemDt2(SprGenItm spr, Date dt2) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setDt2(dt2);
    }

    /**
     * Почистить во всех элементах % выполения
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void clearPercent() {
        sprGenItmDao.findAll().forEach(t -> {
            t.setProc(0D);
            t.setDt1(null);
            t.setDt2(null);
            t.setState(null);
        });
    }

}