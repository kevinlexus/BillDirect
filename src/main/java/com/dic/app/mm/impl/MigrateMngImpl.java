package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dic.app.mm.MigrateMng;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;
import com.dic.bill.dto.SumRecMg;
import com.dic.bill.dto.SumUslOrgRec;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис для миграции данных в другие структуры
 * @author Lev
 *
 */
@Slf4j
@Service
public class MigrateMngImpl implements MigrateMng {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private SaldoUslDAO saldoUslDao;

	/**
	 * Перенести данные из таблиц Директ, в систему учета долгов
	 * по услуге, организации, периоду
	 * @param lsk - лицевой счет
	 * @param period - как правило предыдущий период, относительно текущего
	 */
	@Override
	public void migrateDeb(String lsk, Integer period) {

		// свернуть задолженность, учитывая переплату
		List<SumDebMgRec> lstDeb = getRolledDeb(lsk, period);

		// получить исходящее сальдо предыдущего периода
		List<SumDebUslMgRec> lstSal = getSal(lsk, period);
		log.info("Итого Сальдо={}", lstSal.stream().map(t-> t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add));

		lstDeb.stream()
		.forEach(t-> {
				log.info("mg={}, summa={}", t.getMg(), t.getSumma());
			});
		log.info("Итого Долг={}", lstDeb.stream().map(t-> t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add));

		// получить начисление по услугам и орг., по всем периодам задолжности
		List<SumDebUslMgRec> lstChrg = getChrg(lsk, lstDeb, lstSal);
		lstChrg.forEach(t-> {
			log.info("Начисление: mg={}, usl={}, org={}, summa={}, weigth={}",
					t.getMg(), t.getUslId(), t.getOrgId(), t.getSumma(), t.getWeigth());
		});
		log.info("Итого Weigth={}", lstChrg.stream().map(t-> t.getWeigth()).reduce(BigDecimal.ZERO, BigDecimal::add));


		// Распределить долг
		// вначале положительные суммы

		// результат распределения
		List<SumDebUslMgRec> lstDebResult = new ArrayList<SumDebUslMgRec>();
		// продолжать цикл распределение?
		boolean isContinue = true;
		// не может распределиться?
		boolean isCantDist = true;
		while (isContinue) {
			isContinue = false;
			BigDecimal sumDistAmnt = BigDecimal.ZERO;
			// перебирать долги
			for (SumDebMgRec t : lstDeb.stream()
						.filter(t-> t.getSign() > 0) // положительные
						.filter(t-> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
						.collect(Collectors.toList())
						) {
				// распределить, добавить сумму
				sumDistAmnt = sumDistAmnt.add(distrib(t, lstChrg, lstSal, lstDebResult));
				// продолжать
				isContinue = true;
			}
			if (sumDistAmnt.compareTo(BigDecimal.ZERO) == 0) {
				// не продолжать
				isContinue = false;
				isCantDist = true;
			}
		}
		if (isCantDist) {
			log.info("НЕВОЗМОЖНО РАСПРЕДЕЛИТЬ!");
		}

		lstDeb.stream()
		.forEach(d-> {
				log.info("Долг: mg={}, summa={}", d.getMg(), d.getSumma());
			});
		lstSal.forEach(d-> {
			log.info("Сальдо: mg={}, usl={}, org={}, summa={}",
					d.getMg(), d.getUslId(), d.getOrgId(), d.getSumma());
		});
			log.info("Распределение завершено!");

	}


	/**
	 * Распределить сумму долга по сальдо
	 * @param deb - строка долга
	 * @param lstChrg - начисления по периодам
	 * @param lstSal - сальдо по услугам и орг.
	 * @param lstResult - результат распределения
	 * @return
	 */
	private BigDecimal distrib(SumDebMgRec deb,
			List<SumDebUslMgRec> lstChrg, List<SumDebUslMgRec> lstSal,
			List<SumDebUslMgRec> lstResult) {
		// итоговая сумма распределения
		BigDecimal sumDistAmnt = BigDecimal.ZERO;
		Integer period = deb.getMg();
		List<SumDebUslMgRec> lst = lstChrg.stream()
			.filter(t-> t.getMg().equals(period))
			.filter(t-> t.getWeigth().compareTo(BigDecimal.ZERO) > 0) // вес больше нуля
			.collect(Collectors.toList());

			for (SumDebUslMgRec t: lst) {
				BigDecimal summaDist;
				BigDecimal weigth = t.getWeigth();
				if (weigth.compareTo(new BigDecimal("100")) == 0) {
					// указан эксклюзивный вес, взять сразу весь долг
					summaDist = deb.getSumma();
					// убрать вес
					t.setWeigth(BigDecimal.ZERO);
				} else if (weigth.compareTo(deb.getSumma()) <= 0) {
					// сумма веса меньше или равна сумме долга
					summaDist = t.getWeigth();
				} else {
					// взять всю оставшуюся сумму долга
					summaDist = deb.getSumma();
				}
				// найти запись с данным uslId и orgId в сальдо
				SumDebUslMgRec foundSal = lstSal.stream()
					.filter(d -> d.getUslId().equals(t.getUslId()))
					.filter(d -> d.getOrgId().equals(t.getOrgId()))
					.findAny().orElse(null);

				if (summaDist.compareTo(foundSal.getSumma()) > 0) {
					// сумма для распределения больше суммы сальдо
					// взять всю оставшуюся сумму сальдо
					summaDist = foundSal.getSumma();
				}

				if (summaDist.compareTo(BigDecimal.ZERO) > 0) {
					// уменьшить сумму сальдо
					foundSal.setSumma(foundSal.getSumma().subtract(summaDist));
					// уменьшить сумму долга
					deb.setSumma(deb.getSumma().subtract(summaDist));
					// записать в результат
					// найти запись результата
					SumDebUslMgRec foundResult = lstResult.stream()
							.filter(d -> d.getMg().equals(t.getMg()))
							.filter(d -> d.getUslId().equals(t.getUslId()))
							.filter(d -> d.getOrgId().equals(t.getOrgId()))
							.findAny().orElse(null);
					if (foundResult == null) {
						// не найден результат с такими усл.+орг.+период - создать строку
						lstResult.add(SumDebUslMgRec.builder()
								.withMg(period)
								.withUslId(t.getUslId())
								.withOrgId(t.getOrgId())
								.withSumma(summaDist).build());
					} else {
						// найден результат - добавить сумму
						foundResult.setSumma(foundResult.getSumma().add(summaDist));
					}
				}
				// добавить итоговую сумму распределения
				sumDistAmnt = sumDistAmnt.add(summaDist);
			}
			return sumDistAmnt;
	}

	/**
	 * Получить исходящее сальдо предыдущего периода
	 * @param lsk - лиц.счет
	 * @param period - период
	 * @return
	 */
	private List<SumDebUslMgRec> getSal(String lsk, Integer period) {
		List<SumDebUslMgRec> lst =
				new ArrayList<SumDebUslMgRec>();
		List<SumUslOrgRec> lst2 =
				saldoUslDao.getSaldoUslByLsk(lsk, period);
		lst2.forEach(d-> {
			lst.add(SumDebUslMgRec.builder()
					.withUslId(d.getUslId())
					.withOrgId(d.getOrgId())
					.withSumma(d.getSumma())
					.withSign(d.getSumma().compareTo(BigDecimal.ZERO))
					.build()
					);
		});
		return lst;
	}

	/**
	 * Получить начисление, по всем периодам задолжности
	 * учитывая вес по суммам
	 * только те услуги и организации, которые есть в сальдо!
	 * @param lsk - лиц.счет
	 * @param lstDeb - задолженности по периодам
	 * @param lstSal - сальдо по услугам и орг., для фильтра
	 * @return
	 */
	private List<SumDebUslMgRec> getChrg(String lsk, List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal) {
		List<SumDebUslMgRec> lst
			= new ArrayList<SumDebUslMgRec>();
		//BigDecimal amnt = BigDecimal.ZERO;
		// загрузить начисление по всем периодам задолженности
		lstDeb.forEach(t-> {
			List<SumUslOrgRec> lst2 =
					saldoUslDao.getChargeNaborByLsk(lsk, t.getMg());
			// заполнить по каждому периоду задолженности - строки начисления
			lst2.forEach(d-> {
				lst.add(SumDebUslMgRec.builder()
									.withMg(t.getMg())
									.withUslId(d.getUslId())
									.withOrgId(d.getOrgId())
									.withSumma(d.getSumma())
									.build()
						);
			});
		});

		// итого
		BigDecimal amnt = lst.stream().map(t->t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add);
		// установить коэфф сумм по отношению к итогу

		Iterator<SumDebUslMgRec> itr = lst.iterator();
		while (itr.hasNext()) {
			SumDebUslMgRec t = itr.next();
			Double proc = t.getSumma().doubleValue() / amnt.doubleValue() * 10;
			// округлить и если меньше 0, то принять как 0.10 руб.
			BigDecimal procD = new BigDecimal(proc);
			procD = procD.setScale(2, RoundingMode.HALF_UP);
			if (procD.compareTo(BigDecimal.ZERO) == 0) {
				t.setWeigth(new BigDecimal("0.1"));
			} else {
				t.setWeigth(procD);
			}
			// найти запись с данным uslId и orgId в сальдо
			SumDebUslMgRec foundSal = lstSal.stream()
				.filter(d -> d.getUslId().equals(t.getUslId()))
				.filter(d -> d.getOrgId().equals(t.getOrgId()))
				.findAny().orElse(null);
			if (foundSal == null) {
				// не найдено, удалить элемент
				itr.remove();
			}
		}
		// установить вес эксклюзивного распределения = 100, в отношении усл. и орг.
		// которые находятся только в одном периоде
		lst.forEach(t-> {
			SumDebUslMgRec found = lst.stream()
				.filter(d-> d.getUslId().equals(t.getUslId())
						&& d.getOrgId().equals(t.getOrgId())) // одинаковые усл.+орг.
				.filter(d-> !d.getMg().equals(t.getMg())) // разные периоды
				.findFirst().orElse(null);
			if (found == null) {
				// не найдено в других периодах
				// установить макисмальный вес
				t.setWeigth(new BigDecimal("100"));
			}
		});

		return lst;
	}

	/**
	 * Свернуть задолженность, учитывая переплату
	 * @param lsk - лиц.счет
	 * @param period - период
	 * @return
	 */
	private List<SumDebMgRec> getRolledDeb(String lsk, Integer period) {
		// получить отсортированный список задолженностей по периодам (по предыдущему периоду)
		List<SumRecMg> lst =
				saldoUslDao.getVchargePayByLsk(lsk, period);
		ListIterator<SumRecMg> itr = lst.listIterator();
		// переплата
		BigDecimal ovrPay = BigDecimal.ZERO;
		List<SumDebMgRec> lstDeb = new ArrayList<SumDebMgRec>();
		// свернуть задолженность
		while (itr.hasNext()) {
			SumRecMg t = itr.next();
			// переплата
			// взять сумму текущего периода, добавить переплату
			BigDecimal summa = t.getSumma().add(ovrPay);
			if (summa.compareTo(BigDecimal.ZERO) <= 0) {
				// переплата или 0
				if (itr.hasNext()) {
					// перенести переплату в следующий период
					ovrPay = summa;
				} else {
					// последний период, записать сумму с учетом переплаты
					ovrPay = BigDecimal.ZERO;
					lstDeb.add(SumDebMgRec.builder()
					.withMg(t.getMg())
					.withSumma(summa)
					.withSign(-1)
					.build());
				}
			} else {
				// остался долг, записать его
				ovrPay = BigDecimal.ZERO;
				lstDeb.add(SumDebMgRec.builder()
				.withMg(t.getMg())
				.withSign(1)
				.withSumma(summa).build());
			}
		}
		return lstDeb;
	}



}