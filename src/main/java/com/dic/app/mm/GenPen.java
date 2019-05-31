package com.dic.app.mm;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.*;
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
	// текущая рассчитываемая дата
	private Date curDt;
	// лицевой счет
	private Kart kart;

	// сгруппированные задолженности
	private List<SumDebRec> lst;
	// услуга
	private Usl usl;

	/**
	 * Конструктор
	 * @param kart - лиц.счет
	 * @param uslOrg - услуга + организация
	 * @param usl - услуга (возникла необходимость получить еще и типы услуг, поэтому ввел usl)
	 * @param calcStore - хранилище справочников
	 */
	public GenPen(Kart kart, UslOrg uslOrg, Usl usl, CalcStore calcStore) {
		this.calcStore = calcStore;
		this.uslOrg = uslOrg;
		this.kart = kart;
		this.usl = usl;
	}

	/**
	 * Инициализация класса
	 * @param curDt - дата расчета
	 */
	public void setUp(Date curDt) {
		lst = new ArrayList<>();
		this.curDt = curDt;
	}

	/**
	 * Получить строку даты начала пени по типу лиц.счета
	 * @param mg - период задолженности
	 * @param kart - лиц.счет
	 */
	private SprPen getPenDt(Integer mg, Kart kart) {
		return calcStore.getLstSprPen().stream()
				.filter(t-> t.getTp().equals(kart.getTp()) && t.getMg().equals(mg)) // фильтр по типу лиц.сч. и периоду
				.filter(t-> t.getReu().equals(kart.getUk().getReu()))
				.findFirst().orElse(null);
	}

	/**
	 * свернуть задолженность (учесть переплаты предыдущих периодов)
	 * и расчитать пеню
	 * @param isLastDay - последний ли расчетный день? (для получения итогового долга)
	 */
	public List<SumDebRec> getRolledDebPen(boolean isLastDay) {
		// отсортировать по периоду
		List<SumDebRec> lstSorted = lst.stream()
				.sorted(Comparator.comparing(SumDebRec::getMg))
				.collect(Collectors.toList());

/*		log.info("");
		lstSorted.forEach(t-> {
			log.info("НЕсвернутые долги: дата={} период={}", Utl.getStrFromDate(curDt, "dd.MM.yyyy"), t.getMg());
			log.info("долг для пени={}, долг={}, свернутый долг={}",
					t.getChrg(), t.getSummaDeb(), t.getDebRolled());
		});*/

		// свернуть задолженность
		BigDecimal ovrPay = BigDecimal.ZERO;
		BigDecimal ovrPayDeb = BigDecimal.ZERO;

		Iterator<SumDebRec> itr = lstSorted.iterator();
		while (itr.hasNext()) {
			SumDebRec t = itr.next();
			// дата расчета
			t.setDt(curDt);
			if (uslOrg!=null) {
				// Id услуги
				t.setUslId(uslOrg.getUslId());
				// Id организации
				t.setOrgId(uslOrg.getOrgId());
			}
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
					Pen pen = getPen(summa, t.getMg(), kart);
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
	 * @param kart - лиц.счет
	 */
	private Pen getPen(BigDecimal summa, Integer mg, Kart kart) {
		SprPen penDt = getPenDt(mg,  kart);
		// вернуть кол-во дней между датой расчета пени и датой начала пени по справочнику
		if (penDt == null) {
			if (mg.compareTo(calcStore.getPeriod()) > 0) {
				// период больше текущего, не должно быть пени
				return null;
			} else {
				// некритическая ошибка отсутствия записи в справочнике пени, просто не начислить пеню!
				log.error("ОШИБКА во время начисления пени по лс="+kart.getLsk()+", возможно не настроен справочник PEN_DT!"
						+ "Попытка найти элемент: mg="+mg+", usl.tp_pen_dt="+usl.getTpPenDt()+", kart.reu="+kart.getUk().getReu());
				return null;
			}
		}
		int days = Utl.daysBetween(penDt.getDt(), curDt);
		if (days > 0) {
			// пеня возможна, если есть кол-во дней долга
			//log.info(" spr={}, cur={}, days={}", sprPenUsl.getTs(), curDt, days);
			Stavr penRef = calcStore.getLstStavr().stream()
					.filter(t-> t.getTp().equals(kart.getTp())) // фильтр по типу лиц.счета
					.filter(t-> days >= t.getDays1() && days <= t.getDays2()) // фильтр по кол-ву дней долга
					.filter(t-> Utl.between(curDt, t.getDt1(), t.getDt2())) // фильтр по дате расчета в справочнике
					.findFirst().orElse(null);
			Pen pen = new Pen();
			// рассчет пени = долг * процент/100
			assert penRef != null;
			pen.proc = penRef.getProc();
			pen.penya = summa.multiply(pen.proc).divide(new BigDecimal(100), RoundingMode.HALF_UP);
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
				str2, str, rec.getMg(), rec.getChrg(), rec.getSummaDeb(), rec.getDebRolled());*/
	}

	public List<SumDebRec> getLst() {
		return lst;
	}


}
