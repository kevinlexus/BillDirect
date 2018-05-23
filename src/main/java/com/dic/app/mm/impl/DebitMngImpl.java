package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.DebitMng;
import com.dic.app.mm.GenPen;
import com.dic.bill.Config;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebUslDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.SprPenUslDAO;
import com.dic.bill.dao.StavrUslDAO;
import com.dic.bill.dao.VchangeDetDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.SumRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.ric.bill.Utl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DebitMngImpl implements DebitMng {

	@Autowired
	private Config config;
	@Autowired
	private DebUslDAO debUslDao;
	@Autowired
	private ChargeDAO chargeDao;
	@Autowired
	private VchangeDetDAO vchangeDetDao;
	@Autowired
	private KwtpDayDAO kwtpDayDao;
	@Autowired
	private CorrectPayDAO correctPayDao;
	@Autowired
	private SprPenUslDAO sprPenUslDao;
	@Autowired
	private StavrUslDAO stavrUslDao;
    @PersistenceContext
    private EntityManager em;


	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет, если отсутствует - весь фонд
	 * @param genDt - дата расчета
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public void genDebitAll(String lsk, Date genDt) {
		log.info("Расчет задолженности - НАЧАЛО!");

		// загрузить справочники
		CalcStore calcStore = new CalcStore();
		// лиц.счет
		//calcStore.setLsk(lsk);
		// начальная дата расчета
		calcStore.setDt1(config.getCurDt1());
		// дата расчета пени
		calcStore.setGenDt(genDt);
		// текущий период
		calcStore.setPeriod(Integer.valueOf(config.getPeriod()));
		// период - месяц назад
		calcStore.setPeriodBack(Integer.valueOf(config.getPeriodBack()));
		// справочник дат начала пени
		calcStore.setLstSprPenUsl(sprPenUslDao.findAll());
		// справочник ставок рефинансирования
		calcStore.setLstStavrUsl(stavrUslDao.findAll());

		// вызвать расчет задолженности и пени
		Kart kart = em.find(Kart.class, lsk);
		genDebit(kart, calcStore);

		log.info("Расчет задолженности - ОКОНЧАНИЕ!");
	}


	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет
	 * @param calcStore - хранилище справочников
	 * @param genDt - дата расчета
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
	public void genDebit(Kart kart, CalcStore calcStore) {
		log.info("Расчет задолженности по лиц.счету - НАЧАЛО!");
		String lsk = kart.getLsk();
		// период - месяц назад
		Integer periodBack = calcStore.getPeriodBack();
		// загрузить все финансовые операции по лиц.счету
		// задолжность предыдущего периода - 1
		List<SumRec> lstFlow = debUslDao.getDebitByLsk(lsk, periodBack);
		// текущее начисление - 2
		lstFlow.addAll(chargeDao.getChargeByLsk(lsk));
		// перерасчеты - 5
		lstFlow.addAll(vchangeDetDao.getVchangeDetByLsk(lsk));
		// оплата долга - 3
		lstFlow.addAll(kwtpDayDao.getKwtpDaySumByLsk(lsk));
		// оплата пени - 4
		lstFlow.addAll(kwtpDayDao.getKwtpDayPenByLsk(lsk));
		// корректировки оплаты - 6
		lstFlow.addAll(correctPayDao.getCorrectPayByLsk(lsk));

		calcStore.setLstFlow(lstFlow);

		// сгруппировать до услуги и организации
		List<UslOrg> lstUslOrg = lstFlow.stream()
				.map(t-> new UslOrg(t.getUslId(), t.getOrgId()))
				.distinct().collect(Collectors.toList());
/*		lstUslOrg.forEach(t-> {
			log.info("distinct usl={}, org={}", t.uslId, t.orgId);
		});
*/

		List<SumDebRec> lst = lstUslOrg.stream()
				.filter(t-> t.getUslId().equals("005") && t.getOrgId().equals(10))
				.flatMap(t -> genDebitUsl(kart, t, calcStore).stream())
				.collect(Collectors.toList());

		log.info("Расчет задолженности по лиц.счету - ОКОНЧАНИЕ!");
	}

	/**
	 * Расчет задолжности и пени по услуге
	 * @param kart - лицевой счет
	 * @param u - услуга и организация
	 * @param calcStore - хранилище параметров и справочников
	 * @return
	 */
	private List<SumDebRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore) {
		Date dt1 = calcStore.getDt1();
		Date dt2 = calcStore.getGenDt();
		List<SumRec> lstFlow = calcStore.getLstFlow();
		List<SumDebRec> lstDeb = new ArrayList<SumDebRec>();
		// РАСЧЕТ по дням
		Calendar c = Calendar.getInstance();
		for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
			Date curDt = c.getTime();
			log.info("****** Расчет задолженности по услуге uslId={}, организации orgId={} на дату={}", u.getUslId(), u.getOrgId(), curDt);
			// задолженность предыдущего периода, текущее начисление
			lstDeb = lstFlow.stream()
					.filter(t-> Utl.in(t.getTp(), 1,2))
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), t.getMg(), t.getTp()))
					.collect(Collectors.toList());
			// перерасчеты, включая текущий день
			lstDeb.addAll(lstFlow.stream()
					.filter(t-> Utl.in(t.getTp(), 5) && t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), t.getMg(), t.getTp()))
					.collect(Collectors.toList()));
			// вычесть оплату долга и корректировки оплаты - для расчета долга, включая текущий день (Не включая для задолжности для расчета пени)
			lstDeb.addAll(lstFlow.stream()
					.filter(t-> Utl.in(t.getTp(), 3,6) && t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(
							t.getDt().getTime() < curDt.getTime() || t.getTp().equals(6) ?  // (Не включая текущий день, для задолжности для расчета пени)
																						    // если корректировки - взять любой день
									t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO ,
							t.getDt().getTime() <= curDt.getTime() || t.getTp().equals(6) ? // (включая текущий день, для обычной задолжности)
																						    // если корректировки - взять любой день
									t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO,
							t.getMg(), t.getTp()))
							.collect(Collectors.toList()));
			// сгруппировать задолженности
			GenPen grpSum = new GenPen(kart, u, curDt, calcStore);
			lstDeb.forEach(t-> grpSum.addRec(t));
			// свернуть долги (учесть переплаты предыдущих периодов)
			grpSum.rollDebForPen();

			/*grpSum.getLst().forEach(t-> {
				log.info("DEB: на дату={}, usl={}, org={}, mg={}, долг для пени={}, долг={}, свернутый долг={}, пеня={}",
						curDt, u.getUslId(), u.getOrgId(), t.getMg(), t.getSumma(), t.getSummaDeb(), t.getSummaRollDeb(),  t.getPenya());
			});*/

		}
		return lstDeb;

	}




}