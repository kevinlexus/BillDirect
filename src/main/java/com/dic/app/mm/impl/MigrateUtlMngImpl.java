package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dic.app.mm.Cnt;
import com.dic.app.mm.MigrateUtlMng;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;
import com.dic.bill.dto.SumRecMg;
import com.dic.bill.dto.SumUslOrgRec;

import lombok.extern.slf4j.Slf4j;

/**
 * Вспомогательные методы для сервиса для миграции данных в другие структуры
 * @author Lev
 *
 */
@Slf4j
@Service
public class MigrateUtlMngImpl implements MigrateUtlMng {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private SaldoUslDAO saldoUslDao;



	/**
	 * Распечатать начисление
	 * @param lstChrg
	 */
	@Override
	public void printChrg(List<SumDebUslMgRec> lstChrg) {
		lstChrg.forEach(t-> {
			log.info("Начисление: mg={}, usl={}, org={}, summa={}, weigth={}",
					t.getMg(), t.getUslId(), t.getOrgId(), t.getSumma(), t.getWeigth());
		});
	}


	/**
	 * Распечатать задолженность
	 * @param lstDeb
	 */
	@Override
	public void printDeb(List<SumDebMgRec> lstDeb) {
		lstDeb.stream()
		.forEach(t-> {
				log.info("Долг: mg={}, summa={}, sign={}", t.getMg(), t.getSumma(), t.getSign());
			});
		log.info("Итого Долг={}", lstDeb.stream().map(t-> t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add));
	}

	/**
	 * Распечатать сальдо
	 * @param lstDeb
	 */
	@Override
	public void printSal(List<SumDebUslMgRec> lstSal) {
		lstSal.stream()
		.forEach(t-> {
				log.info("Сальдо: usl={}, org={}, summa={}, sign={}",
						t.getUslId(), t.getOrgId(), t.getSumma(), t.getSign());
			});
	}

	/**
	 * Распечатать результат долгов
	 * @param lstDeb
	 */
	@Override
	public void printDebResult(
			List<SumDebUslMgRec> lstDebResult) {
		lstDebResult.stream()
		.filter(t->t.getSumma().compareTo(BigDecimal.ZERO) !=0)
		.forEach(t-> {
				log.info("Результат: mg={}, usl={}, org={}, summa={}, sign={}",
						t.getMg(), t.getUslId(), t.getOrgId(), t.getSumma(), t.getSign());
			});
	}

	@Override
	public void distDebFinal(Integer period, List<SumDebUslMgRec> lstDebResult, Cnt cnt, String lsk) {
		// получить любую строку итоговых долгов
		SumDebUslMgRec someResult = lstDebResult.stream()
				.findAny().orElse(null);

		// найти нераспределённые положительные суммы долгов
		for (SumDebMgRec t : cnt.lstDebNd.stream().filter(t-> t.getSign()==1).collect(Collectors.toList())) {

			// получить период из итоговых долгов
			SumDebUslMgRec lastResult = lstDebResult.stream()
					.filter(d->d.getUslId().equals(someResult.getUslId()))
					.filter(d->d.getOrgId().equals(someResult.getOrgId()))
					.filter(d->d.getMg().equals(t.getMg()))
					.findFirst().orElse(null);
			if (lastResult != null) {
				// найден последний период - источника
				// поставить сумму
				lastResult.setSumma(
						lastResult.getSumma().add(t.getSumma())
						);
			} else {
				if (someResult == null) {
					throw new RuntimeException("ОШИБКА! Возможно некорректные долги по лиц.счету="+lsk);
				}
				// не найден последний период
				// поставить сумму
				lstDebResult.add(SumDebUslMgRec.builder()
						.withUslId(someResult.getUslId())
						.withOrgId(someResult.getOrgId())
						.withMg(t.getMg())
						.withSumma(t.getSumma())
						.build());
			}
			// списать сумму
			t.setSumma(BigDecimal.ZERO);
		}

		// найти нераспределённые отрицательные суммы долгов
		for (SumDebMgRec t : cnt.lstDebNd.stream().filter(t-> t.getSign()==-1).collect(Collectors.toList())) {
			// получить период из итоговых долгов
			SumDebUslMgRec lastResult = lstDebResult.stream()
					.filter(d->d.getUslId().equals(someResult.getUslId()))
					.filter(d->d.getOrgId().equals(someResult.getOrgId()))
					.filter(d->d.getMg().equals(t.getMg()))
					.findFirst().orElse(null);
			if (lastResult != null) {
				// найден последний период - источника
				// снять сумму
				lastResult.setSumma(
						lastResult.getSumma().subtract(t.getSumma())
						);
			} else {
				if (someResult == null) {
					throw new RuntimeException("ОШИБКА! Возможно некорректные долги по лиц.счету="+lsk);
				}
				// не найден последний период
				// снять сумму
				lstDebResult.add(SumDebUslMgRec.builder()
						.withUslId(someResult.getUslId())
						.withOrgId(someResult.getOrgId())
						.withMg(t.getMg())
						.withSumma(t.getSumma().multiply(new BigDecimal("-1")))
						.build());
			}
			// списать сумму
			t.setSumma(BigDecimal.ZERO);
		}
	}

	@Override
	public void distSalFinal(Integer period, List<SumDebUslMgRec> lstDebResult, Cnt cnt) {
		// сальдо не распределено, долги распределены
		// найти нераспределённые положительные суммы по сальдо
		for (SumDebUslMgRec t : cnt.lstSalNd.stream().filter(t-> t.getSign()==1).collect(Collectors.toList())) {

			// получить последний период из итоговых долгов
			SumDebUslMgRec lastResult = lstDebResult.stream()
					.filter(d->d.getUslId().equals(t.getUslId()))
					.filter(d->d.getOrgId().equals(t.getOrgId()))
					.filter(d->d.getMg().equals(period))
					.findFirst().orElse(null);
			if (lastResult != null) {
				// найден последний период - источника
				// поставить сумму
				lastResult.setSumma(
						lastResult.getSumma().add(t.getSumma())
						);
			} else {
				// не найден последний период
				// поставить сумму
				lstDebResult.add(SumDebUslMgRec.builder()
						.withMg(period)
						.withUslId(t.getUslId())
						.withOrgId(t.getOrgId())
						.withSumma(t.getSumma())
						.build());
			}
			// списать сумму
			t.setSumma(BigDecimal.ZERO);
		}

		// найти нераспределённые отрицательные суммы
		for (SumDebUslMgRec t : cnt.lstSalNd.stream().filter(e-> e.getSign()==-1).collect(Collectors.toList())) {
			// получить последний период из итоговых долгов
			SumDebUslMgRec lastResult = lstDebResult.stream()
					.filter(d->d.getUslId().equals(t.getUslId()))
					.filter(d->d.getOrgId().equals(t.getOrgId()))
					.filter(d->d.getMg().equals(period))
					.findFirst().orElse(null);
			if (lastResult != null) {
				// найден последний период - источника
				// снять сумму
				lastResult.setSumma(
						lastResult.getSumma().subtract(t.getSumma())
						);
			} else {
				// не найден последний период
				// снять сумму
				lstDebResult.add(SumDebUslMgRec.builder()
						.withMg(period)
						.withUslId(t.getUslId())
						.withOrgId(t.getOrgId())
						.withSumma(t.getSumma().multiply(new BigDecimal("-1")))
						.build());
			}
			// списать сумму
			t.setSumma(BigDecimal.ZERO);
		}
	}

	/**
 	 * проверить наличие нераспределённых сумм в сальдо
	 * @param lstSal
	 * @param lstDeb
	 * @param cntSal
	 * @param cntDeb
	 * @return
	 */
	@Override
	public void check(List<SumDebUslMgRec> lstSal, List<SumDebMgRec> lstDeb, Cnt cnt) {
		cnt.lstSalNd = lstSal.stream()
			.filter(t-> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые суммы
			.collect(Collectors.toList());

		cnt.lstSalNd.forEach(t-> {
				log.info("Найдена нераспределенная сумма в САЛЬДО, по uslId={}, orgId={}, summa= {}, sign={}",
								t.getUslId(), t.getOrgId(),	t.getSumma(), t.getSign());
			});
		cnt.cntSal = cnt.lstSalNd.size();

		// проверить наличие нераспределённых сумм в долгах
		cnt.lstDebNd = lstDeb.stream()
			.filter(t-> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые суммы
			.collect(Collectors.toList());

		cnt.lstDebNd.forEach(t-> {
			log.info("Найдена нераспределенная сумма в ДОЛГАХ, по mg={}, summa= {}, sign={}",
							t.getMg(),	t.getSumma(), t.getSign());
		});
		cnt.cntDeb = cnt.lstDebNd.size();
	}

	/**
	 * Проверить нераспределившиеся суммы
	 */
	@Override
	public void checkSumma(List<SumDebUslMgRec> lstSal, List<SumDebMgRec> lstDeb, String lsk) {
		BigDecimal summaSal =
				lstSal.stream()
				.map(t->t.getSumma().multiply(BigDecimal.valueOf(t.getSign())))
				.reduce(BigDecimal.ZERO, BigDecimal::add);
		BigDecimal summaDeb =
				lstDeb.stream()
				.map(t->t.getSumma().multiply(BigDecimal.valueOf(t.getSign())))
				.reduce(BigDecimal.ZERO, BigDecimal::add);

		if (summaSal.compareTo(summaDeb) != 0 ) {
			throw new RuntimeException("Сумма распределения положительных сумм - некорректна! лиц.сч.="+lsk);
		}

	}



	/**
	 * получить периоды в которых было начисление по данной услуге+орг
	 *
	 * @param uslId - код усл.
	 * @param orgId - код орг.
	 * @param lstChrg - начисление
	 * @return
	 */
	@Override
	public List<SumDebUslMgRec> getPeriod(String uslId, Integer orgId, List<SumDebUslMgRec> lstChrg) {
		return lstChrg.stream().filter(t-> t.getUslId().equals(uslId) && t.getOrgId().equals(orgId))
			.collect(Collectors.toList());
	}

	/**
	 * Получить исходящее сальдо предыдущего периода
	 * @param lsk - лиц.счет
	 * @param period - период
	 * @return
	 */
	@Override
	public List<SumDebUslMgRec> getSal(String lsk, Integer period) {
		List<SumDebUslMgRec> lst =
				new ArrayList<SumDebUslMgRec>();
		List<SumUslOrgRec> lst2 =
				saldoUslDao.getSaldoUslByLsk(lsk, period);
		lst2.forEach(d-> {
			lst.add(SumDebUslMgRec.builder()
					.withUslId(d.getUslId())
					.withOrgId(d.getOrgId())
					.withSumma(d.getSumma().abs()) // абсолютное значение
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
	@Override
	public List<SumDebUslMgRec> getChrg(String lsk, List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal) {
		List<SumDebUslMgRec> lst
			= new ArrayList<SumDebUslMgRec>();
		//BigDecimal amnt = BigDecimal.ZERO;
		// загрузить начисление по всем периодам задолженности
		lstDeb.forEach(t-> {
			// получить записи начисления из XITOG3
			List<SumUslOrgRec> lst2 =
					saldoUslDao.getChargeXitog3ByLsk(lsk, t.getMg());
			if (lst2.size() == 0) {
				log.info("Нет записей начисления за период mg={}, в XITOG3, получить из ARCH_CHARGES", t.getMg());
				// нет записей в XITOG3, получить из ARCH_CHARGES
				lst2 = saldoUslDao.getChargeNaborByLsk(lsk, t.getMg());
			} else {
				log.info("Получены записи начисления за период mg={}, из XITOG3", t.getMg());
			}
			// заполнить по каждому периоду задолженности - строки начисления
			lst2.forEach(d-> {
				lst.add(SumDebUslMgRec.builder()
									.withMg(t.getMg())
									.withUslId(d.getUslId())
									.withOrgId(d.getOrgId())
									.withSumma(d.getSumma())
									.withWeigth(BigDecimal.ZERO)
									.withIsSurrogate(false)
									.build()
						);
			});
		});

		return lst;
	}


	/**
	 * Установить веса
	 * @param lstSal - сальдо
	 * @param lstChrg - начисление с весами по усл. + орг.
	 * @param sign - распределить числа (1-положит., -1 - отрицат.)
	 */
	@Override
	public void setWeigths(List<SumDebUslMgRec> lstSal,
			List<SumDebUslMgRec> lstChrg, int sign) {
		Iterator<SumDebUslMgRec> itr = lstChrg.iterator();
		// удалить суррогатные строки начисления
		while (itr.hasNext()) {
			SumDebUslMgRec t = itr.next();
			if (t.getIsSurrogate()) {
				itr.remove();
			}
		}
		// итого
		BigDecimal amnt =
				lstChrg.stream().map(t->t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add);
		// установить коэфф сумм по отношению к итогу и удалить суррогатные строки
		itr = lstChrg.iterator();
		while (itr.hasNext()) {
			SumDebUslMgRec t = itr.next();
			Double proc = t.getSumma().doubleValue() / amnt.doubleValue() * 10;
			// округлить и если меньше 0, то принять как 0.01 руб.
			BigDecimal procD = new BigDecimal(proc);
			procD = procD.setScale(2, RoundingMode.HALF_UP);
			if (procD.compareTo(BigDecimal.ZERO) == 0) {
				t.setWeigth(new BigDecimal("0.01"));
			} else {
				t.setWeigth(procD);
			}
			// найти запись с данным uslId и orgId и sign в сальдо
			SumDebUslMgRec foundSal = lstSal.stream()
				.filter(d -> d.getUslId().equals(t.getUslId()))
				.filter(d -> d.getOrgId().equals(t.getOrgId()))
				.filter(d -> d.getSign().equals(sign))
				.findAny().orElse(null);
			if (foundSal == null) {
				// не найдено, убрать вес
				t.setWeigth(BigDecimal.ZERO);
			}
		}
		// установить вес эксклюзивного распределения = 100, в отношении усл. и орг.
		// которые находятся только в одном периоде
		lstChrg.forEach(t-> {
			SumDebUslMgRec found = lstChrg.stream()
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
	}


	/**
	 * Получить тип задолженности
	 * @param lstDeb - входящая задолженность
	 * @return 1 - только задолж., -1 - только переплаты
	 * 0 - смешанные долги
	 */
	@Override
	public int getDebTp(List<SumDebMgRec> lstDeb) {
		// кол-во положительных чисел
		long positive = lstDeb.stream()
			.filter(t-> t.getSign().equals(1))
			.map(t-> t.getSumma())
			.count();
		// кол-во отрицательных чисел
		long negative = lstDeb.stream()
				.filter(t-> t.getSign().equals(-1))
				.map(t-> t.getSumma())
				.count();
		int tp = -1;
		if (positive > 0L && negative == 0L) {
			// только задолженности
			tp = 1;
		} else if (positive == 0L && negative > 0L) {
			// только переплаты
			tp = -1;
		} else if (positive > 0L && negative > 0L) {
			// задолженности и переплаты (смешанное)
			tp = 0;
		}
		return tp;
	}

	/**
	 * Получить задолженность
	 * @param lsk - лиц.счет
	 * @param period - период
	 * @return
	 */
	@Override
	public List<SumDebMgRec> getDeb(String lsk, Integer period) {
		// получить отсортированный список задолженностей по периодам (по предыдущему периоду)
		List<SumRecMg> lst =
				saldoUslDao.getVchargePayByLsk(lsk, period);
		List<SumDebMgRec> lstDeb = new ArrayList<SumDebMgRec>();
		lst.forEach(t-> {
			lstDeb.add(SumDebMgRec.builder()
					.withMg(t.getMg())
					.withSumma(t.getSumma().abs()) // абсолютное значение
					.withSign(t.getSumma().compareTo(BigDecimal.ZERO))
					.build()
					);
		});
		return lstDeb;
	}


	/**
	 * Записать в результат долга
	 * @param lstDebResult - результаты долгов
	 * @param period - период
	 * @param uslId - код.усл.
	 * @param orgId - код.орг.
	 * @param summa - сумма
	 * @param sign - знак суммы
	 */
	@Override
	public void insDebResult(List<SumDebUslMgRec> lstDebResult, Integer period,
				String uslId, Integer orgId, BigDecimal summa, int sign) {
		// записать в результат
		// найти запись результата
		SumDebUslMgRec foundResult = lstDebResult.stream()
				.filter(d -> d.getMg().equals(period))
				.filter(d -> d.getUslId().equals(uslId))
				.filter(d -> d.getOrgId().equals(orgId))
				.filter(d -> d.getSign().equals(sign))
				.findAny().orElse(null);
		if (foundResult == null) {
			// не найден результат с такими усл.+орг.+период - создать строку
			lstDebResult.add(SumDebUslMgRec.builder()
					.withMg(period)
					.withUslId(uslId)
					.withOrgId(orgId)
					.withSign(sign)
					.withSumma(summa).build());
		} else {
			// найден результат - добавить сумму
			foundResult.setSumma(foundResult.getSumma().add(summa));
		}

	}


	/**
	 * Подготовить суррогатную строку начисления
	 * @param lstDeb - долги
	 * @param lstSal - сальдо
	 * @param lstChrg - начисление
	 * @param sign - знак долга
	 */
	@Override
	public void addSurrogateChrg(List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal, List<SumDebUslMgRec> lstChrg,
			int sign) {
		// добавить нужный период в строку начисления
		// найти все не распределенные долги
		lstDeb.stream()
			.filter(t-> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
			.filter(t-> t.getSign().equals(sign) ) // знак долга
			.forEach(d-> {
				// найти записи с нераспределёнными долгами, в сальдо
				lstSal.stream()
					.filter(t-> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевое
					.filter(t -> t.getSign().equals(sign)) // знак сальдо
					.forEach(t-> {
						// вес 1 руб., чтоб быстрее распределялось
						BigDecimal weigth = new BigDecimal("1.00");
						// добавить суррогатную запись начисления, чтоб распределялось
						log.info("В начисление добавлена суррогатная запись: mg={}, usl={}, org={}, weigth={}",
								d.getMg(), t.getUslId(), t.getOrgId(), weigth);
						lstChrg.add(SumDebUslMgRec.builder()
								.withMg(d.getMg())
								.withUslId(t.getUslId())
								.withOrgId(t.getOrgId())
								.withWeigth(weigth)
								.withIsSurrogate(true)
								.build()
								);
					});

			});
	}


}