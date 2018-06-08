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
import com.dic.bill.model.scott.PenDt;
import com.dic.bill.model.scott.PenRef;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

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
	// текущая рассчитываемая дата
	private Date curDt;
	// лицевой счет
	private Kart kart;

	// сгруппированные задолженности
	List<SumDebRec> lst = new ArrayList<SumDebRec>();
	// услуга
	private Usl usl;

	/**
	 * Конструктор
	 * @param kart - лиц.счет
	 * @param uslOrg - услуга + организация
	 * @param usl - услуга (возникла необходимость получить еще и типы услуг, поэтому ввел usl)
	 * @param curDt - рассчетная дата
	 * @param calcStore - хранилище справочников
	 */
	public GenPen(Kart kart, UslOrg uslOrg, Usl usl, Date curDt, CalcStore calcStore) {
		this.calcStore = calcStore;
		this.uslOrg = uslOrg;
		this.curDt = curDt;
		this.kart = kart;
		this.usl = usl;
	};

	/**
	 * Справочник начала действия пени... была попытка сделать на нём @Cacheable
	 * но не получилось. Нужно было бы тогда перенести его в public метод другого бина,
	 * да и GenPen тогда пришлось бы тоже сделать @Service... что по архитектурным причинам не просто... TODO
	 * Подумать!
	 * @param mg
	 * @param uslId
	 * @param ukId
	 * @return
	 */
	private PenDt getPenDt(Integer mg, Usl usl, Kart kart) {
		return calcStore.getLstPenDt().stream()
				.filter(t-> t.getUslTpPen().equals(usl.getTpPenDt()) && t.getMg().equals(mg)) // фильтр по типу услуги и периоду
				.filter(t-> Utl.between2(kart.getUk().getReu(), t.getReuFrom(), t.getReuTo())) // фильтр по УК
				.findFirst().orElse(null);
	}

	/**
	 * свернуть задолженность (учесть переплаты предыдущих периодов)
	 * и расчитать пеню
	 * @param isLastDay - последний ли расчетный день? (для получения итогового долга)
	 * @return
	 * @throws ErrorWhileChrgPen
	 */
	public List<SumDebRec> getRolledDebPen(boolean isLastDay) throws ErrorWhileChrgPen {
		// отсортировать по периоду
		List<SumDebRec> lstSorted = lst.stream().sorted((t1, t2) ->
			t1.getMg().compareTo(t2.getMg())).collect(Collectors.toList());

/*		log.info("");
		lstSorted.forEach(t-> {
			log.info("НЕсвернутые долги: дата={} период={}", Utl.getStrFromDate(curDt, "dd.MM.yyyy"), t.getMg());
			log.info("долг для пени={}, долг={}, свернутый долг={}",
					t.getSumma(), t.getSummaDeb(), t.getDebRolled());
		});*/

		// свернуть задолженность
		BigDecimal ovrPay = BigDecimal.ZERO;
		BigDecimal ovrPayDeb = BigDecimal.ZERO;

		Iterator<SumDebRec> itr = lstSorted.iterator();
		while (itr.hasNext()) {
			SumDebRec t = itr.next();
			// дата расчета
			t.setDt(curDt);
			// Id услуги
			t.setUslId(uslOrg.getUslId());
			// Id организации
			t.setOrgId(uslOrg.getOrgId());
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
						// сохранить текущую пеню
						t.setPenyaChrg(pen.penya);
						// кол-во дней просрочки
						t.setDays(pen.days);
						// % расчета пени
						t.setProc(pen.proc);
					}
				}
			}

			if (isLastDay) {
				// Актуально для последнего расчетного дня
				// Задолженность для отображения клиенту (свернутая)
				// взять сумму текущего периода, добавить переплату
				summa = t.getDebRolled().add(ovrPayDeb);
				if (summa.compareTo(BigDecimal.ZERO) <= 0) {
					// переплата или 0
					if (itr.hasNext()) {
						// перенести переплату в следующий период
						ovrPayDeb = summa;
						t.setDebRolled(BigDecimal.ZERO);
					} else {
						// последний период, записать сумму с учетом переплаты
						ovrPayDeb = BigDecimal.ZERO;
						t.setDebRolled(summa);
					}
				} else {
					// остался долг, записать его
					ovrPayDeb = BigDecimal.ZERO;
					t.setDebRolled(summa);
				}
			}

		}
		if (calcStore.getDebugLvl().equals(1)) {
			log.info("");
			lstSorted.forEach(t-> {
				log.info("СВЕРНУТЫЕ долги: дата={} период={}", Utl.getStrFromDate(curDt, "dd.MM.yyyy"), t.getMg());
				log.info("долг для пени={}, долг={}, свернутый долг={}, пеня вх.сал.={}, "
						+ "пеня тек.={}, корр.пени={}, дней просрочки={}, % пени={}",
						t.getSumma(), t.getDebOut(), t.getDebRolled(), t.getPenyaIn(),
						t.getPenyaChrg(), t.getPenyaCorr(), t.getDays(), t.getProc());
			});
		}

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
	 * @throws ErrorWhileChrgPen
	 */
	private Pen getPen(BigDecimal summa, Integer mg) throws ErrorWhileChrgPen {
		PenDt penDt = getPenDt(mg,  usl,  kart);
		// вернуть кол-во дней между датой расчета пени и датой начала пени по справочнику
		if (penDt == null) {
			if (mg.compareTo(calcStore.getPeriod())==1) {
				// период больше текущего, не должно быть пени
				return null;
			} else {
				// некритическая ошибка отсутствия записи в справочнике пени, просто не начислить пеню!
				log.error("ОШИБКА во время начисления пени по лс="+kart.getLsk()+", возможно не настроен справочник C_SPR_PEN_USL!"
						+ "Попытка найти элемент: mg="+mg+", usl.tp_pen_dt="+usl.getTpPenDt()+", kart.reu="+kart.getUk().getReu());
				return null;
			}
		}
		int days = Utl.daysBetween(penDt.getDt(), curDt);
		if (days > 0) {
			// пеня возможна, если есть кол-во дней долга
			//log.info(" spr={}, cur={}, days={}", sprPenUsl.getDt(), curDt, days);
			PenRef penRef = calcStore.getLstPenRef().stream()
					.filter(t-> t.getUslTpRef().equals(usl.getTpPenRef())) // фильтр по типу услуги
					.filter(t-> days >= t.getDays1().intValue() && days <= t.getDays2().intValue()) // фильтр по кол-ву дней долга
					.filter(t-> Utl.between(curDt, t.getDt1(), t.getDt2())) // фильтр по дате расчета в справочнике
					.findFirst().orElse(null);
			Pen pen = new Pen();
			// рассчет пени = долг * процент/100
			pen.proc = penRef.getProc();
			pen.penya = summa.multiply(pen.proc).divide(new BigDecimal(100));
			pen.days = days;
			return pen;
		} else {
			// нет пени
			return null;
		}
	}



	// добавление и группировка финансовой операции, для получения сгруппированной задолжности по периоду
	public void addRec(SumDebRec rec, Boolean isLastDay) {

		if (rec.getMg() == null) {
			// если mg не заполнено, установить - текущий период (например для начисления)
			rec.setMg(calcStore.getPeriod());
		}
		if (lst.size() == 0) {
			lst.add(rec);
		} else {
			// добавить суммы финансовой операции
			SumDebRec foundRec = lst.stream().filter(t->t.getMg().equals(rec.getMg()))
				.findFirst().orElse(null);
			if (foundRec == null) {
				lst.add(rec);
			} else {
				// для долга для расчета пени
				foundRec.setSumma(foundRec.getSumma().add(rec.getSumma()));
				// для долга как он есть в базе
				foundRec.setDebOut(foundRec.getDebOut().add(rec.getDebOut()));
				// для свернутого долга
				foundRec.setDebRolled(foundRec.getDebRolled().add(rec.getDebRolled()));
				// для отчета
				if (isLastDay) {
					// вх.сальдо по задолженности
					foundRec.setDebIn(foundRec.getDebIn().add(rec.getDebIn()));
					// вх.сальдо по пене
					foundRec.setPenyaIn(foundRec.getPenyaIn().add(rec.getPenyaIn()));
					// оплата пени
					foundRec.setPenyaPay(foundRec.getPenyaPay().add(rec.getPenyaPay()));
					// начисление
					foundRec.setChrg(foundRec.getChrg().add(rec.getChrg()));
					// перерасчеты
					foundRec.setChng(foundRec.getChng().add(rec.getChng()));
					// корректировки по пене
					foundRec.setPenyaCorr(foundRec.getPenyaCorr().add(rec.getPenyaCorr()));
					// оплата задолженности
					foundRec.setDebPay(foundRec.getDebPay().add(rec.getDebPay()));
					// корректировки оплаты
					foundRec.setPayCorr(foundRec.getPayCorr().add(rec.getPayCorr()));
				}
			}
		}

/*		Для отладки, не удалять:
 * 		String str = null;
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
				str2, str, rec.getMg(), rec.getSumma(), rec.getSummaDeb(), rec.getDebRolled());*/
	}

	public List<SumDebRec> getLst() {
		return lst;
	}


}
