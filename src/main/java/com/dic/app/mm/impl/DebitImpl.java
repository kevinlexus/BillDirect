package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dic.app.mm.DebitMng;
import com.dic.bill.Config;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebUslDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.VchangeDetDAO;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.SumRec;
import com.ric.bill.Utl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DebitImpl implements DebitMng {

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


	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет
	 * @param genDt - дата расчета
	 */
	@Override
	public void genDebit(String lsk, Date genDt) {
		log.info("Расчет задолженности - НАЧАЛО!");
		// текущий период
		Integer period = Integer.valueOf(config.getPeriod());
		// период - месяц назад
		Integer periodBack = Integer.valueOf(config.getPeriodBack());
		// начальная дата расчета
		Date dt1 = config.getCurDt1();
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
				.flatMap(t -> genDebitUsl(t, lstFlow, dt1, genDt, period).stream())
				.collect(Collectors.toList());

		log.info("Расчет задолженности - ОКОНЧАНИЕ!");
	}

	/**
	 * Расчет задолжности и пени по услуге
	 * @param uslOrg - услуга и организация
	 * @param orgId - Id организации
	 * @param lstFlow - финансовые операции за период
	 * @param dt1 - начальная дата расчета
	 * @param genDt - конечная дата расчета (дата на которую расчитать пеню)
	 * @param perioid - текущий период
	 * @return
	 */
	private List<SumDebRec> genDebitUsl(UslOrg u, List<SumRec> lstFlow, Date dt1, Date dt2, Integer period) {
		List<SumDebRec> lstDeb = new ArrayList<SumDebRec>();
		// расчет по дням
		Calendar c = Calendar.getInstance();
		for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
		Date genDt = c.getTime();
		log.info("****** Расчет задолженности по услуге uslId={}, организации orgId={} на дату={}", u.getUslId(), u.getOrgId(), genDt);
		// задолженность предыдущего периода, текущее начисление
		lstDeb = lstFlow.stream()
				.filter(t-> Utl.in(t.getTp(), 1,2))
				.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
				.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), t.getMg(), t.getTp()))
				.collect(Collectors.toList());
		// перерасчеты, включая текущий день
		lstDeb.addAll(lstFlow.stream()
				.filter(t-> Utl.in(t.getTp(), 5) && t.getDt().getTime() <= genDt.getTime())
				.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
				.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), t.getMg(), t.getTp()))
				.collect(Collectors.toList()));
		// вычесть оплату долга и корректировки оплаты - для расчета долга, включая текущий день (Не включая для задолжности для расчета пени)
		lstDeb.addAll(lstFlow.stream()
				.filter(t-> Utl.in(t.getTp(), 3,6) && t.getDt().getTime() <= genDt.getTime())
				.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
				.map(t-> new SumDebRec(
						t.getDt().getTime() < genDt.getTime() || t.getTp().equals(6) ?  // (Не включая текущий день, для задолжности для расчета пени)
																					    // если корректировки - взять любой день
								t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO ,
						t.getDt().getTime() <= genDt.getTime() || t.getTp().equals(6) ? // (включая текущий день, для обычной задолжности)
																					    // если корректировки - взять любой день
								t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO,
						t.getMg(), t.getTp()))
						.collect(Collectors.toList()));
		// сгруппировать задолженности
		GrpSum grpSum = new GrpSum(period);
		lstDeb.forEach(t-> grpSum.addRec(t));
		// свернуть долги (учесть переплаты предыдущих периодов)
		grpSum.roll();



		grpSum.getLst().forEach(t-> {
			log.info("DEB: genDt={}, usl={}, org={}, mg={}, summa={}, summaDeb={}",
					genDt, u.getUslId(), u.getOrgId(), t.getMg(), t.getSumma(), t.getSummaDeb());
		});

		}
		return lstDeb;

	}

	/**
	 * Внутренний класс для группировки услуг и организаций
	 * @author Lev
	 *
	 */
	class UslOrg {

		UslOrg(String uslId, Integer orgId) {
			super();
			this.uslId = uslId;
			this.orgId = orgId;
		}
		private String uslId;
		private Integer orgId;

		public String getUslId() {
			return uslId;
		}

		public void setUslId(String uslId) {
			this.uslId = uslId;
		}

		public Integer getOrgId() {
			return orgId;
		}

		public void setOrgId(Integer orgId) {
			this.orgId = orgId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((orgId == null) ? 0 : orgId.hashCode());
			result = prime * result + ((uslId == null) ? 0 : uslId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			UslOrg other = (UslOrg) obj;
			if (orgId == null) {
				if (other.orgId != null)
					return false;
			} else if (!orgId.equals(other.orgId))
				return false;
			if (uslId == null) {
				if (other.uslId != null)
					return false;
			} else if (!uslId.equals(other.uslId))
				return false;
			return true;
		}

	}

	/**
	 * Внутренний класс для группировки задолженностей
	 * @author Lev
	 *
	 */
	class GrpSum {
		// текущий период
		private Integer period;
		// сгруппированные задолженности
		List<SumDebRec> lst = new ArrayList<SumDebRec>();
		GrpSum(Integer period) {
			this.period = period;
		};

		// свернуть долги (учесть переплаты предыдущих периодов)
		private void roll() {
			// отсортировать по периоду
			List<SumDebRec> lstSorted = lst.stream().sorted((t1, t2) ->
				t1.getMg().compareTo(t2.getMg())).collect(Collectors.toList());


			for (SumDebRec t: lstSorted) {
				log.info("Sort after: mg={}", t.getMg());
			};

		}

		private void addRec(SumDebRec rec) {
			if (rec.getMg() == null) {
				// если mg не заполнено, установить - текущий период (например для начисления)
				rec.setMg(period);
			}
			if (lst.size() == 0) {
				lst.add(rec);
			} else {
				// добавить суммы задолженности
				SumDebRec foundRec = lst.stream().filter(t->t.getMg().equals(rec.getMg()))
					.findFirst().orElse(null);
				if (foundRec == null) {
					lst.add(rec);
				} else {
					foundRec.setSumma(foundRec.getSumma().add(rec.getSumma()));
					foundRec.setSummaDeb(foundRec.getSummaDeb().add(rec.getSummaDeb()));
				}
			}

			String str = null;
			String str2 = "Добавлено";
			switch (rec.getTp()) {
			case 1 :
				str = "задолжность предыдущего периода";
				break;
			case 2 :
				str = "текущее начисление";
				break;
			case 3 :
				str = "оплата долга";
				str2 = "Вычтено";
				break;
			case 4 :
				str = "оплата пени (не считать в итог долга)";
				break;
			case 5 :
				str = "перерасчет";
				break;
			case 6 :
				str = "корректировка оплаты";
				str2 = "Вычтено";
				break;
			}
			log.info("{}: {}, mg={}, summa={}, summaDeb={}", str2, str, rec.getMg(), rec.getSumma(), rec.getSummaDeb());
		}

		public List<SumDebRec> getLst() {
			return lst;
		}

	}

}