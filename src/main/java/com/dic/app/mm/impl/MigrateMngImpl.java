package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.MigrateMng;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;
import com.dic.bill.dto.SumRecMg;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.model.scott.Deb;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.CommonResult;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistDeb;

import lombok.extern.slf4j.Slf4j;


class Cnt {
	long cntSal = 0L;
	long cntDeb = 0L;
	List<SumDebUslMgRec> lstSalNd;
	List<SumDebMgRec> lstDebNd;
}

/**
 * Сервис для миграции данных в другие структуры
 * @author Lev
 *
 */
@Slf4j
@Service
@Scope("prototype")
public class MigrateMngImpl implements MigrateMng {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private SaldoUslDAO saldoUslDao;
	@Autowired
	private DebDAO debDao;
	@Autowired
	private ConfigApp config;
	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private ThreadMng<String> threadMng;


	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor=Exception.class)
	public void migrateAll(String lskFrom, String lskTo) throws ErrorWhileDistDeb {
		long startTime = System.currentTimeMillis();
		log.info("НАЧАЛО миграции задолженности в новые структуры");

		// получить список лицевых счетов
		List<String> lstItem;
		// флаг - заставлять ли многопоточный сервис проверять маркер остановки главного процесса
		boolean isCheckStop = false;
		lstItem= saldoUslDao.getAllWithNonZeroDeb(lskFrom, lskTo,
				config.getPeriodBack());

		// будет выполнено позже, в создании потока
		PrepThread<String> reverse = (item) -> {
			// сервис миграции задолженностей
			MigrateMng migrateMng = ctx.getBean(MigrateMng.class);
			return migrateMng.migrateDeb(item,
					Integer.parseInt(config.getPeriodBack()),
					Integer.parseInt(config.getPeriod()));
		};

		// вызвать в потоках
		try {
			threadMng.invokeThreads(reverse, 15, lstItem, isCheckStop);
		} catch (InterruptedException | ExecutionException e) {
			log.error(Utl.getStackTraceString(e));
			throw new ErrorWhileDistDeb("ОШИБКА во время миграции задолженности!");
		}


		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("ОКОНЧАНИЕ миграции задолженности - Общее время выполнения={}", totalTime);

	}

	/**
	 * Перенести данные из таблиц Директ, в систему учета долгов
	 * по услуге, организации, периоду
	 * @param lsk - лицевой счет
	 * @param periodBack - как правило предыдущий период, относительно текущего
	 * @param period - текущий период
	 * @throws ErrorWhileDistDeb
	 */
	@Async
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor=Exception.class)
	public Future<CommonResult> migrateDeb(String lsk, Integer periodBack, Integer period) {

		log.info("Распределение лиц.счета={}, period={}, periodBack={}", lsk, period, periodBack);
		// получить задолженность
		List<SumDebMgRec> lstDeb = getDeb(lsk, periodBack);

		// получить исходящее сальдо предыдущего периода
		List<SumDebUslMgRec> lstSal = getSal(lsk, period);
		log.info("Итого Сальдо={}", lstSal.stream().map(t-> t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add));


		// получить начисление по услугам и орг., по всем периодам задолжности
		List<SumDebUslMgRec> lstChrg = getChrg(lsk, lstDeb, lstSal);
		// распечатать долг
		//printDeb(lstDeb);
		// распечатать начисление
		//printChrg(lstChrg);

		// РАСПРЕДЕЛЕНИЕ
		// результат распределения
		List<SumDebUslMgRec> lstDebResult = new ArrayList<SumDebUslMgRec>();

		log.info("*** ДОЛГ до распределения:");
		printDeb(lstDeb);
		printSal(lstSal, lstDebResult);
		log.info("");

		// получить тип долга
		int debTp = getDebTp(lstDeb);
		if (debTp==1 || debTp==-1) {
			// только задолженности или только переплаты

			// распределить сперва все положительные или отрицательные числа
			// зависит от типа задолженности
			int sign = debTp;
			log.info("*** РАСПРЕДЕЛИТЬ долги одного знака, по sign={}", sign);
			boolean res = distDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, true, false);

			if (!res) {
				// не удалось распределить, распределить принудительно
				// добавив нужный период в строку с весом 1.00 руб, в начисления
				addSurrogateChrg(lstDeb, lstSal, lstChrg, sign);
				// вызвать еще раз распределение, не устанавливая веса
				res = distDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, false, false);
			}
		} else if (debTp==0) {
			// смешанные долги
			log.info("*** РАСПРЕДЕЛИТЬ смешанные суммы долги и перплаты");

			// распечатать долг
			printSal(lstSal, lstDebResult);
			printDeb(lstDeb);

			// распределить сперва все ДОЛГИ
			int sign = 1;
			log.info("*** РАСПРЕДЕЛИТЬ сперва ДОЛГИ");
			boolean res = distDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, true, false);
			// распечатать долг
			printDeb(lstDeb);
			printSal(lstSal, lstDebResult);

			if (!res) {
				// не удалось распределить, распределить принудительно
				// добавив нужный период в строку с весом 1.00 руб, в начисления
				addSurrogateChrg(lstDeb, lstSal, lstChrg, sign);
				// вызвать еще раз распределение, не устанавливая веса
				log.info("*** НЕ УДАЛОСЬ РАСПРЕДЕЛИТЬ ДОЛГИ до конца, повтор!");
				res = distDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, false, false);
				// распечатать долг
				printDeb(lstDeb);
				printSal(lstSal, lstDebResult);
			}

			// распределить все ПЕРЕПЛАТЫ
			sign = -1;
			log.info("*** РАСПРЕДЕЛИТЬ ПЕРЕПЛАТЫ");
			res = distDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, true, false);
			// распечатать долг
			printDeb(lstDeb);
			printSal(lstSal, lstDebResult);

			if (!res) {
				// не удалось распределить, распределить принудительно
				// добавив нужный период в строку с весом 1.00 руб, в начисления
				addSurrogateChrg(lstDeb, lstSal, lstChrg, sign);
				// вызвать еще раз распределение, не устанавливая веса
				log.info("*** НЕ УДАЛОСЬ РАСПРЕДЕЛИТЬ ПЕРЕПЛАТУ до конца, повтор!");
				res = distDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, false, false);
				// распечатать долг
			}
		}

		printDeb(lstDeb);
		printSal(lstSal, lstDebResult);

		// проверить наличие распределения
		Cnt cnt = new Cnt();
		check(lstSal, lstDeb, cnt);

		log.info("*** cntSal={}, cntDeb={}", cnt.cntSal, cnt.cntDeb);
		if (cnt.cntSal == 0L && cnt.cntDeb != 0L) {
			// сальдо распределено, долги не распределены
			// вызвать принудительное распределение
			log.info("*** РАСПРЕДЕЛИТЬ ДОЛГИ финально");
			distDebFinal(periodBack, lstDebResult, cnt, lsk);

		} else if (cnt.cntSal != 0L && cnt.cntDeb == 0L) {
			// сальдо не распределено, долги распределены

			log.info("*** РАСПРЕДЕЛИТЬ САЛЬДО финально");
			distSalFinal(periodBack, lstDebResult, cnt);

		} else if (cnt.cntSal != 0L && cnt.cntDeb != 0L) {
			// сальдо не распределено, долги не распределены
			distDebFinal(periodBack, lstDebResult, cnt, lsk);
			distSalFinal(periodBack, lstDebResult, cnt);
		}

		// проверить наличие распределения
		check(lstSal, lstDeb, cnt);

		if (cnt.cntDeb != 0L) {
			// долги не распределены
			throw new RuntimeException("ОШИБКА #1 не распределены ДОЛГИ в лс="+lsk);
		}
		if (cnt.cntSal != 0L) {
			// сальдо не распределено
			throw new RuntimeException("ОШИБКА #2 не распределено САЛЬДО в лс="+lsk);
		}

		printDeb(lstDeb);
		printSal(lstSal, lstDebResult);

		// удалить предыдущее распределение
		debDao.delByLskPeriod(lsk, periodBack);
		// сохранить распределённые задолженности
		Kart kart = em.find(Kart.class, lsk);
		for (SumDebUslMgRec t : lstDebResult) {
			Usl usl = em.find(Usl.class, t.getUslId());
			if (usl==null) {
				throw new RuntimeException("ОШИБКА #4 сохранения задолженности, не найдена услуга usl="+t.getUslId());
			}
			Org org = em.find(Org.class, t.getOrgId());
			if (org==null) {
				throw new RuntimeException("ОШИБКА #5 сохранения задолженности, не найдена организация org="+t.getOrgId());
			}
			// сохранить новое
			Deb deb = Deb.builder()
				.withKart(kart)
				.withDebOut(t.getSumma())
				.withMg(t.getMg())
				.withUsl(usl)
				.withOrg(org)
				.withMgFrom(periodBack)
				.withMgTo(periodBack)
				.build();
			em.persist(deb);
		}

		log.info("Распределение по лиц.счету={} выполнено!", lsk);
		CommonResult res = new CommonResult(lsk, 1111111111); // TODO 111111
		return new AsyncResult<CommonResult>(res);
	}

	private void distDebFinal(Integer period, List<SumDebUslMgRec> lstDebResult, Cnt cnt, String lsk) {
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
					log.error("ОШИБКА! Возможно некорректные долги по лиц.счету={}", lsk);
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

	private void distSalFinal(Integer period, List<SumDebUslMgRec> lstDebResult, Cnt cnt) {
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
	private void check(List<SumDebUslMgRec> lstSal, List<SumDebMgRec> lstDeb, Cnt cnt) {
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
	 * Распечатать начисление
	 * @param lstChrg
	 */
	private void printChrg(List<SumDebUslMgRec> lstChrg) {
		lstChrg.forEach(t-> {
			log.info("Начисление: mg={}, usl={}, org={}, summa={}, weigth={}",
					t.getMg(), t.getUslId(), t.getOrgId(), t.getSumma(), t.getWeigth());
		});
	}


	/**
	 * Распечатать задолженность
	 * @param lstDeb
	 */
	private void printDeb(List<SumDebMgRec> lstDeb) {
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
	private void printSal(List<SumDebUslMgRec> lstSal,
			List<SumDebUslMgRec> lstDebResult) {
		lstSal.stream()
		.forEach(t-> {
				log.info("Сальдо: usl={}, org={}, summa={}, sign={}",
						t.getUslId(), t.getOrgId(), t.getSumma(), t.getSign());
			});
		lstDebResult.stream()
		.forEach(t-> {
				log.info("Результат: usl={}, org={}, summa={}",
						t.getUslId(), t.getOrgId(), t.getSumma());
			});
	}

	/**
	 * Подготовить сурргогатную строку начисления
	 * @param lstDeb - долги
	 * @param lstSal - сальдо
	 * @param lstChrg - начисление
	 * @param sign - знак долга
	 */
	private void addSurrogateChrg(List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal, List<SumDebUslMgRec> lstChrg,
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


	/**
	 * Распределить по долгам
	 * @param lstDeb - долги по периодам
	 * @param lstSal - сальдо по усл.+орг.
	 * @param lstChrg - начисления по периодам
	 * @param lstDebResult - результат
	 * @param sign - знак распределения
	 * @param isSetWeigths - повторно установить веса?
	 * @param isForced - принудительное? (не будет смотреть на сальдо)
	 * @return
	 */
	private boolean distDeb(List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal, List<SumDebUslMgRec> lstChrg,
			List<SumDebUslMgRec> lstDebResult, int sign, boolean isSetWeigths, boolean isForced) {
		if (isSetWeigths) {
			// установить веса по начислению
			setWeigths(lstSal, lstChrg, sign);
		}
		// продолжать цикл распределение?
		boolean isContinue = true;
		// не может распределиться?
		boolean isCantDist = true;
		while (isContinue) {
			isContinue = false;
			BigDecimal sumDistAmnt = BigDecimal.ZERO;
			// перебирать долги
			for (SumDebMgRec t : lstDeb.stream()
						.filter(t-> t.getSign().equals(sign) ) // знак долга
						.filter(t-> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
						.collect(Collectors.toList())
						) {
				// распределить, добавить сумму
				sumDistAmnt = sumDistAmnt.add(
						distByPeriodSal(t, lstChrg, lstSal, lstDebResult, sign, isForced)
						);
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
			return false;
		}
		return true;
	}


	/**
	 * Распределить сумму долга по сальдо
	 * @param deb - строка долга
	 * @param lstChrg - начисления по периодам
	 * @param lstSal - сальдо по услугам и орг.
	 * @param lstDebResult - результат распределения
	 * @param sign - распределить числа (1-положит., -1 - отрицат.)
 	 * @param isForced - принудительное? (не будет смотреть на сальдо)
	 * @return
	 */
	private BigDecimal distByPeriodSal(SumDebMgRec deb,
			List<SumDebUslMgRec> lstChrg, List<SumDebUslMgRec> lstSal,
			List<SumDebUslMgRec> lstDebResult, int sign, boolean isForced) {
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
				// log.info("CHECK usl={}, org={} sign={}", t.getUslId(), t.getOrgId(), sign);
				SumDebUslMgRec foundSal = null;
				if (!isForced) {
					foundSal = lstSal.stream()
							.filter(d -> d.getUslId().equals(t.getUslId()))
							.filter(d -> d.getOrgId().equals(t.getOrgId()))
							.filter(d -> d.getSign().equals(sign)) // знак сальдо
							.findAny().orElse(null);
					BigDecimal foundSalsumma;
					if (foundSal!=null) {
						foundSalsumma = foundSal.getSumma();
					} else {
						foundSalsumma = BigDecimal.ZERO;
					}
					if (summaDist.compareTo(foundSalsumma) > 0) {
						// сумма для распределения больше суммы сальдо
						// взять всю оставшуюся сумму сальдо
						summaDist = foundSalsumma;
					}
				}

				if (summaDist.compareTo(BigDecimal.ZERO) > 0) {
					// уменьшить сумму сальдо, если не форсированное распределение
					if (!isForced) {
						foundSal.setSumma(foundSal.getSumma().subtract(summaDist));
					}
					// уменьшить сумму долга
					deb.setSumma(deb.getSumma().subtract(summaDist));
					// записать в результат
					// найти запись результата
					SumDebUslMgRec foundResult = lstDebResult.stream()
							.filter(d -> d.getMg().equals(t.getMg()))
							.filter(d -> d.getUslId().equals(t.getUslId()))
							.filter(d -> d.getOrgId().equals(t.getOrgId()))
							.findAny().orElse(null);
					if (foundResult == null) {
						// не найден результат с такими усл.+орг.+период - создать строку
						// сумма с учетом знака!
						lstDebResult.add(SumDebUslMgRec.builder()
								.withMg(period)
								.withUslId(t.getUslId())
								.withOrgId(t.getOrgId())
								.withSumma(
										summaDist.multiply(BigDecimal.valueOf(sign)
												)).build());
					} else {
						// найден результат - добавить или вычесть сумму
						// зависит от знака!
						foundResult.setSumma(foundResult.getSumma().add(
								summaDist.multiply(BigDecimal.valueOf(sign)
										)));
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
	private void setWeigths(List<SumDebUslMgRec> lstSal,
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
			// округлить и если меньше 0, то принять как 0.10 руб.
			BigDecimal procD = new BigDecimal(proc);
			procD = procD.setScale(2, RoundingMode.HALF_UP);
			if (procD.compareTo(BigDecimal.ZERO) == 0) {
				t.setWeigth(new BigDecimal("0.1"));
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
	private int getDebTp(List<SumDebMgRec> lstDeb) {
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
	private List<SumDebMgRec> getDeb(String lsk, Integer period) {
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
	 * Получить свернутую задолженность - НЕ ИСПОЛЬЗОВАТЬ
	 * @param lst - входящая задолженность
	 * @return
	 */
	private List<SumDebMgRec> getRolledDeb(List<SumRecMg> lst) {
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