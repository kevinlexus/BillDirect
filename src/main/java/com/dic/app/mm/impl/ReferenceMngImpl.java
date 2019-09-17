package com.dic.app.mm.impl;

import com.dic.app.mm.ReferenceMng;
import com.dic.bill.dao.RedirPayDAO;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.RedirPay;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Сервис методов обработки справочников
 * @author Lev
 * @version 1.00
 */
@Slf4j
@Service
public class ReferenceMngImpl implements ReferenceMng {

	private final RedirPayDAO redirPayDao;

	@Autowired
	public ReferenceMngImpl(RedirPayDAO redirPayDao) {
		this.redirPayDao = redirPayDao;
	}

	/**
	 * Получить редирект пени
	 * @param uslId - Id услуги
	 * @param orgId - Id организаци
	 * @param kart - лицевой счет
	 * @param tp - тип обработки 1-оплата, 0 - пеня
	 */
	@Override
	@Cacheable(cacheNames="ReferenceMng.getUslOrgRedirect",
			key="{#uslId, #orgId, #kart.getLsk(), #tp}" )
	public UslOrg getUslOrgRedirect(String uslId, Integer orgId, Kart kart, Integer tp) {
		UslOrg uo = new UslOrg(null, null);
		log.info("tp={}, getReu()={}, uslId={}, orgId={}", tp,
				kart.getUk().getReu(), uslId, orgId);
		List<RedirPay> lst = redirPayDao.getRedirPayOrd(tp,
				kart.getUk().getReu(), uslId, orgId) .stream()
			.filter(t->  t.getUk()==null || t.getUk() // либо заполненный УК, либо пуст
					.equals(kart.getUk()))
			.filter(t-> t.getUslSrc()==null || t.getUslSrc().getId() // либо заполненный источник услуги, либо пуст
					.equals(uslId))
			.filter(t-> t.getOrgSrc()==null || t.getOrgSrc().getId() // либо заполненный источник орг., либо пуст
					.equals(orgId))
			.collect(Collectors.toList());
		for (RedirPay t : lst) {
				if (t.getUslDst() != null) {
					// перенаправить услугу
					uo.setUslId(t.getUslDst().getId());
				}
				if (t.getOrgDstId() != null) {
					if (t.getOrgDstId().equals(-1)) {
						// перенаправить на организацию, обслуживающую фонд
						uo.setOrgId(kart.getUk().getId());
					} else {
						// перенаправить на организацию
						uo.setOrgId(t.getOrgDstId());
					}
				}
				if (uo.getUslId() != null &&
						uo.getOrgId() != null) {
					// все замены найдены
					return uo;
				}
		}

		// вернуть замены, если не найдены
		if (uo.getUslId() == null) {
			uo.setUslId(uslId);
		}
		if (uo.getOrgId() == null) {
			uo.setOrgId(orgId);
		}

		return uo;
	}

}
