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

import com.dic.app.mm.GenMng;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.SprGenItm;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class GenMngImpl implements GenMng {

	@PersistenceContext
	private EntityManager em;
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
		case 35:
			// вызов из WebCtrl
			qr = em.createStoredProcedureQuery("scott.p_thread.check_itms");
			qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
			qr.registerStoredProcedureParameter(2, Integer.class, ParameterMode.IN);

			qr.setParameter(1, id);
			qr.setParameter(2, sel);
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
}