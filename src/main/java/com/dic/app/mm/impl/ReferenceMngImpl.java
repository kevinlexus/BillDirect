package com.dic.app.mm.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.RedirPayDAO;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;

/**
 * Сервис методов обработки справочников
 * @author Lev
 * @version 1.00
 */
@Service
public class ReferenceMngImpl implements ReferenceMng {

	@Autowired
	private RedirPayDAO redirPayDao;

	/**
	 * Получить редирект пени
	 * @param uslOrg - услуга + организация
	 * @param kart - лицевой счет
	 * @param tp - тип обработки 1-оплата, 0 - пеня
	 */
	@Override
	public UslOrg getUslOrgRedirect(UslOrg uslOrg, Kart kart, Integer tp) {
		redirPayDao.findAll().stream()
			.filter(t-> t.getTp().equals(tp))
			.filter(t-> t.getUk().getId() // либо заполненный УК, либо пуст
					.equals(kart.getUk().getId()) || t.getUk()==null)
			.filter(t-> t.getUslSrc().getId() // либо заполненный источник услуги, либо пуст
					.equals(uslOrg.getUslId()) || t.getUslSrc()==null)
			.filter(t-> t.getOrgSrc().getId() // либо заполненный источник орг., либо пуст
					.equals(uslOrg.getOrgId()) || t.getOrgSrc()==null)
			.forEach(t-> {

			});
		return uslOrg;

	}

}
