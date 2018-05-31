package com.dic.app.mm.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.RedirPayDAO;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.RedirPay;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис методов обработки справочников
 * @author Lev
 * @version 1.00
 */
@Slf4j
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
		UslOrg uo = new UslOrg(null, null);
		List<RedirPay> lst = redirPayDao.findAll().stream()
			.filter(t-> t.getTp().equals(tp))
			.filter(t->  t.getUk()==null || t.getUk().getId() // либо заполненный УК, либо пуст
					.equals(kart.getUk().getId()))
			.filter(t-> t.getUslSrc()==null || t.getUslSrc().getId() // либо заполненный источник услуги, либо пуст
					.equals(uslOrg.getUslId()))
			.filter(t-> t.getOrgSrc()==null || t.getOrgSrc().getId() // либо заполненный источник орг., либо пуст
					.equals(uslOrg.getOrgId()))
			.collect(Collectors.toList());
		for (RedirPay t : lst) {
				if (t.getUslDst() != null) {
					// перенаправить услугу
/*					log.info("пеня перенаправлена, услуга {}->{}",
							uslOrg.getUslId(), t.getUslDst().getId());
*/					uo.setUslId(t.getUslDst().getId());
				}
				if (t.getOrgDstId() != null) {
					if (t.getOrgDstId().equals(-1)) {
						// перенаправить на организацию, обслуживающую фонд
/*						log.info("пеня перенаправлена, организация {}->УК {}",
								uslOrg.getOrgId(), kart.getUk().getId());
*/						uo.setOrgId(kart.getUk().getId());
					} else {
						// перенаправить на организацию
						uo.setOrgId(t.getOrgDstId());
/*						log.info("пеня перенаправлена, организация {}->{}",
								uslOrg.getOrgId(), t.getOrgDstId());
*/					}
				}
				if (uo.getUslId() != null &&
						uo.getOrgId() != null) {
					// все замены найдены
					return uo;
				}
		}

		// вернуть замены, если не найдены
		if (uo.getUslId() == null) {
			uo.setUslId(uslOrg.getUslId());
		}
		if (uo.getOrgId() == null) {
			uo.setOrgId(uslOrg.getOrgId());
		}

		return uo;
	}

}
