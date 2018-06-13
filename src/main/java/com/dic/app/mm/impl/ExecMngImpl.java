package com.dic.app.mm.impl;

import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.ExecMng;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.SprGenItm;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ExecMngImpl implements ExecMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private ConfigApp config;
	@Autowired
	private SprGenItmDAO sprGenItmDao;

	/**
	 * Обновить элемент меню
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public void updateSprGenItem(List<SprGenItm> lst) {
		lst.forEach(t -> {
			SprGenItm sprGenItm = em.find(SprGenItm.class, t.getId());
			if (t.getSel() != null) {
				sprGenItm.setSel(t.getSel());
			}
			/* Зачем обновлять имя?????? if (t.getName() != null) {
				sprGenItm.setName(t.getName());
			}*/

		});
	}

	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public Integer execProc(Integer var, Integer id, Integer sel) {
		StoredProcedureQuery qr;
		Integer ret = null;
		Integer par = null;

		switch (var) {
		case 4:
		case 5:
		case 6:
		case 7:
		case 38:
			// проверка ошибок
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
		case 8:
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
		case 37:
			// проверка ошибок
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
			break;
		case 16:
			// установить текущую дату, до формирования
			qr = em.createStoredProcedureQuery("scott.init.set_date_for_gen");
			qr.executeUpdate();
			break;
		case 17:
			//чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
			qr = em.createStoredProcedureQuery("scott.p_thread.gen_clear_vol");
			qr.executeUpdate();
			break;
		case 35:
			// вызов из WebCtrl
			qr = em.createStoredProcedureQuery("scott.p_thread.check_itms");
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
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
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
			qr.setParameter(1, id);
			qr.executeUpdate();
			break;
		case 101:
			// распределить ОДН во вводах, где есть ОДПУ
			qr = em.createStoredProcedureQuery("scott.p_thread.gen_dist_odpu");
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
			qr.setParameter(1, id);
			qr.executeUpdate();
			break;
		case 102:
			// начислить пеню по домам
			qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_pen_house");
			// дата, не заполняем, null
			qr.registerStoredProcedureParameter(1, Date.class, ParameterMode.IN);
			// id дома
			qr.registerStoredProcedureParameter(2, Integer.class, ParameterMode.IN);
			qr.setParameter(1, new Date());
			qr.setParameter(2, id);
			qr.executeUpdate();
			break;
		case 103:
			// расчитать начисление по домам
			qr = em.createStoredProcedureQuery("scott.c_charges.gen_charges");
			// id дома
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
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
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
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
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public void setGenDate() {
		execProc(16, null, null);
	}

	/**
	 * Закрыть или открыть базу для пользователей
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public void stateBase(int state) {
		execProc(3, null, state);
	}

	/**
	 * Установить процент выполнения в элементе меню
	 * @param spr - элемент меню
	 * @param proc - процент
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW, rollbackFor=Exception.class)
	public void setPercent(SprGenItm spr, double proc) {
		SprGenItm sprFound=em.find(SprGenItm.class, spr.getId());
		sprFound.setProc(proc);
		// прогресс формирования +1
		config.incProgress();
	}

	/**
	 * Почистить во всех элементах % выполения
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor=Exception.class)
	public void clearPercent() {
		sprGenItmDao.findAll().forEach(t-> {
			t.setProc(0D);
		});
	}

}