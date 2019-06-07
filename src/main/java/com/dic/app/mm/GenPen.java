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
	//private List<SumDebRec> lstGroupedDebt;
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
		//lstGroupedDebt = new ArrayList<>();
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


}
