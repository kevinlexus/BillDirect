package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
import com.dic.bill.dto.SumPenRec;
import com.dic.bill.dto.SumRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.DebPenUsl;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Usl;
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
		long startTime = System.currentTimeMillis();
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

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("Расчет задолженности - ОКОНЧАНИЕ! Время расчета={}", totalTime);
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
		// текущий период
		Integer period = calcStore.getPeriod();
		// период - месяц назад
		Integer periodBack = calcStore.getPeriodBack();
		// загрузить все финансовые операции по лиц.счету
		// задолженность предыдущего периода - 1
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

		List<SumPenRec> lst = lstUslOrg.parallelStream()
				//.filter(t-> t.getUslId().equals("005") && t.getOrgId().equals(10))
				.flatMap(t -> genDebitUsl(kart, t, calcStore).stream())
				.collect(Collectors.toList());

		log.info("");
		// удалить записи текущего периода
		debUslDao.delByLskPeriod(lsk, period);

		// сохранить расчет
		log.info("ИТОГОВАЯ задолжность и пеня");
		lst.forEach(t-> {
			log.info("uslId={}, orgId={}, период={}, долг={}, свернутый долг={}, пеня={} руб., дней просрочки={}",
					t.getUslOrg().getUslId(), t.getUslOrg().getOrgId(), t.getMg(),
					t.getSummaDeb(), t.getSummaRollDeb(), t.getPenya(), t.getDays());
			// сохранить задолжность
			DebPenUsl debPenUsl = DebPenUsl.builder()
								.withKart(kart)
								.withUsl(em.find(Usl.class, t.getUslOrg().getUslId()))
								.withOrg(em.find(Org.class, t.getUslOrg().getOrgId()))
								.withSumma(t.getSummaDeb())
								.withSummaRolled(t.getSummaRollDeb())
								.withPenyaCur(t.getPenya())
								.withPenya(new BigDecimal("7777777777.77"))
								.withDays(t.getDays())
								.withMg(t.getMg())
								.withPeriod(period)
								.build();
			em.persist(debPenUsl);

		});


		log.info("Расчет задолженности по лиц.счету - ОКОНЧАНИЕ!");

	}

	/**
	 * Расчет задолжности и пени по услуге
	 * @param kart - лицевой счет
	 * @param u - услуга и организация
	 * @param calcStore - хранилище параметров и справочников
	 * @return
	 */
	private List<SumPenRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore) {
		// дата начала расчета
		Date dt1 = calcStore.getDt1();
		// дата окончания расчета
		Date dt2 = calcStore.getGenDt();
		List<SumRec> lstFlow = calcStore.getLstFlow();
		List<SumDebRec> lstDeb = new ArrayList<SumDebRec>();
		// РАСЧЕТ по дням
		Calendar c = Calendar.getInstance();
		List<SumDebRec> lstPenAllDays = new ArrayList<SumDebRec>(30);
		for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
			Date curDt = c.getTime();
			//log.info("****** Расчет задолженности по услуге uslId={}, организации orgId={} на дату={}", u.getUslId(), u.getOrgId(), curDt);
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
			GenPen genPen = new GenPen(kart, u, curDt, calcStore);
			// добавить финансовые операции по состоянию на текущий день
			lstDeb.forEach(t-> genPen.addRec(t));
			// является ли текущий день последним расчетным?
			boolean isLastDay = curDt.equals(dt2);
			// свернуть долги (учесть переплаты предыдущих периодов),
			// рассчитать пеню на определенный день, добавить в общую коллекцию по всем дням
			lstPenAllDays.addAll(genPen.getRolledDebPen(isLastDay));
			/*grpSum.getLst().forEach(t-> {
				log.info("DEB: на дату={}, usl={}, org={}, mg={}, долг для пени={}, долг={}, свернутый долг={}, пеня={}",
						curDt, u.getUslId(), u.getOrgId(), t.getMg(), t.getSumma(), t.getSummaDeb(), t.getSummaRollDeb(),  t.getPenya());
			});*/

		}
		// сгруппировать пеню
		return getGroupingPenDeb(u, lstPenAllDays);

	}


	/**
	 * Сгруппировать по периодам пеню, и долги на дату расчета
	 * @param uslOrg - услуга и организация
	 * @param lst - долги по всем дням
	 */
	private List<SumPenRec> getGroupingPenDeb(UslOrg uslOrg, List<SumDebRec> lst) {
		// получить долги на последнюю дату
		List<SumPenRec> lstDebAmnt =  lst.stream()
				.filter(t-> t.getIsLastDay() == true)
				.map(t-> new SumPenRec(uslOrg, t.getSummaDeb(), t.getSummaRollDeb(), null, t.getDays(), t.getMg()))
				.collect(Collectors.toList());

		// сгруппировать начисленную пеню по периодам
		lst.forEach(t-> {
			addPen(uslOrg, lstDebAmnt, t.getMg(), t.getPenya(), t.getDays());
		});
		// округлить начисленную пеню до копеек
		lstDebAmnt.forEach(t-> {
			t.setPenya(t.getPenya().setScale(2, RoundingMode.HALF_UP));
		});

		return lstDebAmnt;
	}

	/**
	 * добавить пеню по периоду в долги по последней дате
	 * @param uslOrg - услуга и организация
	 * @param lstDebAmnt - коллекция долгов
	 * @param mg - период долга
	 * @param penya - начисленая пеня за день
	 * @param days - дней просрочки (если не будет найден период в долгах, использовать данный параметр)
	 */
	private void addPen(UslOrg uslOrg, List<SumPenRec> lstDebAmnt, Integer mg, BigDecimal penya, Integer days) {
		// найти запись долга с данным периодом
		SumPenRec recDeb = lstDebAmnt.stream()
				.filter(t-> t.getMg().equals(mg)).findFirst().orElse(null);
		if (recDeb != null) {
			// запись найдена, сохранить значение пени
			recDeb.setPenya(recDeb.getPenya().add(penya));
		} else {
			// BigDecimal summaDeb, BigDecimal summaRollDeb, BigDecimal penya, Integer days, Integer mg)
			// запись НЕ найдена, создать новую, сохранить значение пени
			lstDebAmnt.add(new SumPenRec(uslOrg, null, null, penya, days, mg));
		}
	}




}