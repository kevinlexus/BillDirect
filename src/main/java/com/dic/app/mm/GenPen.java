package com.dic.app.mm;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.SprPenUsl;
import com.dic.bill.model.scott.StavrUsl;
import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

/**
 * Группировки задолженностей и расчет пени
 * @author Lev
 *
 */
@Slf4j
public class GenPen {
	// хранилище справочников и параметров
	private CalcStore calcStore;
	// текущие услуга и организация
	private UslOrg uslOrg;
	// текущий рассчитываемая дата
	private Date curDt;
	// лицевой счет
	private Kart kart;

	// сгруппированные задолженности
	List<SumDebRec> lst = new ArrayList<SumDebRec>();

	public GenPen(Kart kart, UslOrg uslOrg, Date curDt, CalcStore calcStore) {
		this.calcStore = calcStore;
		this.uslOrg = uslOrg;
		this.curDt = curDt;
		this.kart = kart;
	};

	/**
	 * свернуть задолженность (учесть переплаты предыдущих периодов)
	 * и расчитать пеню
	 * @param isLastDay - последний ли расчетный день? (для получения итогового долга)
	 * @return
	 */
	public List<SumDebRec> getRolledDebPen(boolean isLastDay) {
		// отсортировать по периоду
		List<SumDebRec> lstSorted = lst.stream().sorted((t1, t2) ->
			t1.getMg().compareTo(t2.getMg())).collect(Collectors.toList());

		log.info("");
		lstSorted.forEach(t-> {
			log.info("НЕсвернутые долги: дата={} период={}", Utl.getStrFromDate(curDt, "dd.MM.yyyy"), t.getMg());
			log.info("долг для пени={}, долг={}, свернутый долг={}",
					t.getSumma(), t.getSummaDeb(), t.getSummaRollDeb());
		});

		// свернуть задолженность
		BigDecimal ovrPay = BigDecimal.ZERO;
		BigDecimal ovrPayDeb = BigDecimal.ZERO;

		Iterator<SumDebRec> itr = lstSorted.iterator();
		while (itr.hasNext()) {
			SumDebRec t = itr.next();
			// пометить текущий день, является ли последним расчетным днём
			t.setIsLastDay(isLastDay);
			// Задолженность для расчета ПЕНИ
			// взять сумму текущего периода, добавить переплату
			BigDecimal summa = t.getSumma().add(ovrPay);
			if (summa.compareTo(BigDecimal.ZERO) <= 0) {
				// переплата или 0
				if (itr.hasNext()) {
					// перенести переплату в следующий период
					ovrPay = summa;
					t.setSumma(BigDecimal.ZERO);
				} else {
					// последний период, записать сумму с учетом переплаты
					ovrPay = BigDecimal.ZERO;
					t.setSumma(summa);
				}
			} else {
				// остался долг, записать его
				ovrPay = BigDecimal.ZERO;
				t.setSumma(summa);

				// если установлена дата ограничения пени, не считать пеню, начиная с этой даты
				if (kart.getPnDt() == null || curDt.getTime() < kart.getPnDt().getTime()) {
					// рассчитать пеню и сохранить
					Pen pen = getPen(summa, t.getMg());
					if (pen != null) {
						t.setPenya(pen.penya);
						// кол-во дней просрочки
						t.setDays(pen.days);
						// % расчета пени
						t.setProc(pen.proc);
					}
				}
			}

			// Задолженность для отображения клиенту
			// взять сумму текущего периода, добавить переплату
			summa = t.getSummaRollDeb().add(ovrPayDeb);
			if (summa.compareTo(BigDecimal.ZERO) <= 0) {
				// переплата или 0
				if (itr.hasNext()) {
					// перенести переплату в следующий период
					ovrPayDeb = summa;
					t.setSummaRollDeb(BigDecimal.ZERO);
				} else {
					// последний период, записать сумму с учетом переплаты
					ovrPayDeb = BigDecimal.ZERO;
					t.setSummaRollDeb(summa);
				}
			} else {
				// остался долг, записать его
				ovrPayDeb = BigDecimal.ZERO;
				t.setSummaRollDeb(summa);
			}

		}
		log.info("");
		lstSorted.forEach(t-> {
			log.info("СВЕРНУТЫЕ долги: дата={} период={}", Utl.getStrFromDate(curDt, "dd.MM.yyyy"), t.getMg());
			log.info("долг для пени={}, долг={}, свернутый долг={}, пеня={}, дней просрочки={}, % пени={}",
					t.getSumma(), t.getSummaDeb(), t.getSummaRollDeb(), t.getPenya(), t.getDays(), t.getProc());
		});

		return lstSorted;
	}

	/**
	 * Внутренний класс DTO расчета пени
	 * @author Lev
	 *
	 */
	class Pen {
		// кол-во дней просрочки
		int days=0;
		// рассчитанная пеня
		BigDecimal penya;
		// % по которому рассчитана пеня (информационно)
		BigDecimal proc;
	}
	/**
	 * Рассчитать пеню
	 * @param summa - долг
	 * @param mg - период долга
	 */
	private Pen getPen(BigDecimal summa, Integer mg) {
		SprPenUsl sprPenUsl = calcStore.getLstSprPenUsl().stream()
				.filter(t-> t.getUsl().getId().equals(uslOrg.getUslId()) && t.getMg().equals(mg)) // фильтр по услуге и периоду
				.filter(t-> t.getUk().equals(kart.getUk())) // фильтр по УК
				.findFirst().orElse(null);
		// вернуть кол-во дней между датой расчета пени и датой начала пени по справочнику
		int days = Utl.daysBetween(sprPenUsl.getDt(), curDt);
		if (days > 0) {
			// пеня возможна, если есть кол-во дней долга
			//log.info(" spr={}, cur={}, days={}", sprPenUsl.getDt(), curDt, days);
			StavrUsl stavrUsl = calcStore.getLstStavrUsl().stream()
					.filter(t-> t.getUsl().getId().equals(uslOrg.getUslId())) // фильтр по услуге
					.filter(t-> days >= t.getDays1().intValue() && days <= t.getDays2().intValue()) // фильтр по кол-ву дней долга
					.filter(t-> Utl.between(curDt, t.getDt1(), t.getDt2())) // фильтр по дате расчета в справочнике
					.findFirst().orElse(null);
			// рассчет пени = долг * процент/100
			Pen pen = new Pen();
			pen.proc = stavrUsl.getProc();
			pen.penya = summa.multiply(pen.proc).divide(new BigDecimal(100));
			pen.days = days;
			return pen;
		} else {
			// нет пени
			return null;
		}
	}

	// добавление и группировка финансовой операции, для получения сгруппированной задолжности по периоду
	public void addRec(SumDebRec rec) {
		if (rec.getMg() == null) {
			// если mg не заполнено, установить - текущий период (например для начисления)
			rec.setMg(calcStore.getPeriod());
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
				foundRec.setSummaRollDeb(foundRec.getSummaRollDeb().add(rec.getSummaRollDeb()));
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
		log.info("{}: {}, mg={}, summa={}, summaDeb={}, summaRollDeb={}",
				str2, str, rec.getMg(), rec.getSumma(), rec.getSummaDeb(), rec.getSummaRollDeb());
	}

	public List<SumDebRec> getLst() {
		return lst;
	}

}
