package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Service;

import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.GenPen;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DebitThrMngImpl implements DebitThrMng {

	@PersistenceContext
    private EntityManager em;

	/**
	 * Расчет задолжности и пени по услуге
	 * @param kart - лицевой счет
	 * @param u - услуга и организация
	 * @param calcStore - хранилище параметров и справочников
	 * @return
	 * @throws ErrorWhileChrgPen
	 */
	@Override
	public List<SumDebRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore, CalcStoreLocal localStore) throws ErrorWhileChrgPen {
		//log.info("usl= {}, org= {}", u.getUslId(), u.getOrgId());
		// дата начала расчета
		Date dt1 = calcStore.getDt1();
		// дата окончания расчета
		Date dt2 = calcStore.getGenDt();
		// загрузить услугу
		Usl usl = em.find(Usl.class, u.getUslId());

		// объект расчета пени
		GenPen genPen = new GenPen(kart, u, usl, calcStore, localStore.getReuId());

		List<SumDebRec> lstDeb = new ArrayList<SumDebRec>(50);
		// РАСЧЕТ по дням
		Calendar c = Calendar.getInstance();
		List<SumDebRec> lstPenAllDays = new ArrayList<SumDebRec>(100);
		for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
			Date curDt = c.getTime();
			genPen.setUp(curDt);
			// является ли текущий день последним расчетным?
			boolean isLastDay = curDt.equals(dt2);

			// ЗАГРУЗИТЬ выбранные финансовые операции на ТЕКУЩУЮ дату расчета curDt
			// долги предыдущего периода (вх.сальдо)
			lstDeb = localStore.getLstDebFlow().stream()
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t->
					new SumDebRec(t.getDebOut(), null, null, null, null, null,
							t.getDebOut(), t.getDebOut(), null, null, t.getMg(), t.getTp())
					)
					.collect(Collectors.toList());
			// текущее начисление
			lstDeb.addAll(localStore.getLstChrgFlow().stream()
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(null, null, null, null,
							t.getSumma(), null, t.getSumma(), t.getSumma(), null, null, t.getMg(), t.getTp()))
					.collect(Collectors.toList()));

			// перерасчеты, включая текущий день
			lstDeb.addAll(localStore.getLstChngFlow().stream()
					.filter(t-> t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(null, null, null, null, null, null,
							t.getSumma(), t.getSumma(), null, null, t.getMg(), t.getTp()))
					.collect(Collectors.toList()));
			// вычесть оплату долга - для расчета долга, включая текущий день (Не включая для задолженности для расчета пени)
			// ВНИМАНИЕ! TODO! По оплате долга введена искусственная ошибка, перенесённая из пакета C_CPENYA строка "and case when t.dtek < init.get_cur_dt_start"
			// Считаю некорректным таким образом ограничивать дату поступления оплаты, так как оплата может быть принята 31 числом предыдущего месяца
			// и она должна повлиять на долг. Решить позже, что с этим делать ред. 08.06.2018
			lstDeb.addAll(localStore.getLstPayFlow().stream()
					.filter(t-> t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(null, null, null, t.getSumma(), null, null,
							(t.getDt().getTime() < dt1.getTime() ? dt1.getTime() :t.getDt().getTime()) //<-- ПРЕДНАМЕРЕННАЯ ошибка
									< curDt.getTime() ?  // (Не включая текущий день, для задолжности для расчета пени)
									t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO ,
							t.getDt().getTime() <= curDt.getTime() ? // (включая текущий день, для обычной задолжности)
									t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO,
							null, null,
							t.getMg(), t.getTp()))
							.collect(Collectors.toList()));
			// ВНИМАНИЕ! TODO! По оплате долга введена искусственная ошибка

			// вычесть корректировки оплаты - для расчета долга, включая текущий день
			lstDeb.addAll(localStore.getLstPayCorrFlow().stream()
					.filter(t-> t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(null, null, t.getSumma(), null, null, null,
							t.getSumma().multiply(new BigDecimal("-1")) ,
							t.getSumma().multiply(new BigDecimal("-1")),
							null, null,
							t.getMg(), t.getTp()))
							.collect(Collectors.toList()));

			if (isLastDay) {
				// АКТУАЛЬНО только для последнего дня расчета:
				// вх.сальдо по пене
				lstDeb.addAll(localStore.getLstDebPenFlow().stream()
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, null, null, null, null, null, null, null,
								t.getPenOut(), null, t.getMg(), t.getTp()))
						.collect(Collectors.toList()));

/*				localStore.getLstDebPenFlow().stream()
				.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
				.forEach(t->{
							log.info("ВХОДЯЩЕЕ сальдо по пене mg={}, usl={} org={}, сумма={}", t.getMg(), t.getUslId(), t.getOrgId(), t.getPenOut());

						});

*/				// корректировки начисления пени
				lstDeb.addAll(localStore.getLstPenChrgCorrFlow().stream()
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, null, null, null, null, null, null, null, null,
								t.getSumma(), t.getMg(), t.getTp()))
						.collect(Collectors.toList()));

				// перерасчеты, для отчета (на последнюю дату)
				lstDeb.addAll(localStore.getLstChngFlow().stream()
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, null, null, null, null, t.getSumma(),
								null, null, null, null, t.getMg(), t.getTp()))
						.collect(Collectors.toList()));

				// оплата пени
				lstDeb.addAll(localStore.getLstPayPenFlow().stream()
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, t.getSumma(), null, null, null,
								null, null, null, null, null, t.getMg(), t.getTp()))
						.collect(Collectors.toList()));
			}

			// добавить и сгруппировать все финансовые операции, по состоянию на текущий день
			lstDeb.forEach(t-> genPen.addRec(t, isLastDay));
			// свернуть долги (учесть переплаты предыдущих периодов),
			// рассчитать пеню на определенный день, добавить в общую коллекцию по всем дням
			lstPenAllDays.addAll(genPen.getRolledDebPen(isLastDay));
		}

/*		lstPenAllDays.forEach(t-> {
			log.info("НЕдетализир.: dt={}, mg={}, penChrg={}", t.getDt(), t.getMg(), t.getPenyaChrg());
		});*/

		// сгруппировать пеню и вернуть
		//return getGroupingPenDeb(u, lstPenAllDays);

		// вернуть всю детализированную пеню по данной услуге и организации, по дням
		return lstPenAllDays;
	}


}