package com.dic.app.mm.impl;

import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.collections4.list.TreeList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.dic.app.mm.ComprTbl;
import com.dic.app.mm.MntBase;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.AchargePrepDAO;
import com.dic.bill.dao.AnaborDAO;
import com.dic.bill.dao.ParamDAO;
import com.dic.bill.model.scott.Param;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongTableException;
import com.ric.dto.Result;

import lombok.extern.slf4j.Slf4j;
/**
 * Cервис обслуживания базы данных
 * @author lev
 *
 */
@Slf4j
@Service
public class MntBaseImpl implements MntBase {

	@Autowired
	private AnaborDAO anaborDao;
	@Autowired
	private AchargeDAO achargeDao;
	@Autowired
	private AchargePrepDAO achargePrepDao;
	@Autowired
	private ParamDAO paramDao;
	@Autowired
	private ApplicationContext ctx;
	// параметры
	private Param param;
	// текущий период
	private Integer curPeriod;
	// период -1 от текущего
	private Integer backPeriod;
	// анализировать все периоды?
	private boolean isAllPeriods;

	/**
	 * Сжать таблицу
	 * @throws Exception
	 * @param - tableClass - класс таблицы
	 * @param - firstLsk - начать с лицевого
	 * @param - oneLsk - только этот лицевой
	 * @param - isByUsl - использовать ли поле "usl" для критерия сжатия (не подходит для всех таблиц, например archkart)
	 */
	private void comprTable(String table, String firstLsk, String oneLsk, Boolean isByUsl) throws Exception {
		Integer startTime;
		Integer endTime;
		Integer totalTime;
		// Кол-во потоков, начать от 10, иначе может быть Out of Memory Error, Java heap space, на слабых серверах
		int cntThread = 10;
		// Кол-во потоков, лучшее
		boolean setBestCntThread = false;
		int bestCntThread = 0;
		Integer cnt;
		// каждую пачку исполнять по N раз
		int batchCnt = 1;
		// лучшее время исполнения, мс
		Integer bestTime = 5000;
		Integer batchTime = 0;
		List<Integer> avgLst;

		log.info("Compress table:{},  threads count:{}", table, cntThread);
		startTime = (int) System.currentTimeMillis();

		List<String> lstLsk = null;
		// Получить список лс
		if (oneLsk != null) {
			lstLsk = new ArrayList<String>();
			lstLsk.add(oneLsk);
		} else {

	    	if (table.equals("anabor")) {
				lstLsk = anaborDao.getAfterLsk(firstLsk).stream().map(t -> t.getLsk())
						.collect(Collectors.toList());
	    	} else if (table.equals("acharge")) {
				lstLsk = achargeDao.getAfterLsk(firstLsk).stream().map(t -> t.getLsk())
						.collect(Collectors.toList());
	    	} else if (table.equals("achargeprep")) {
				lstLsk = achargePrepDao.getAfterLsk(firstLsk).stream().map(t -> t.getLsk())
						.collect(Collectors.toList());
	    	} else {
	    		log.error("Л.с.:{} Ошибка! Некорректный класс таблицы:{}", table);
	    		throw new WrongTableException("Некорректный класс таблицы!");
	    	}

		}
		//List<List<String>> batch = Lists.partition(lstLsk, cntThread);
		Queue<String> qu = new LinkedList<>(lstLsk);
			cnt = 1;
			avgLst = new TreeList<Integer>();
			while (true) {
				if (qu.size() ==0) {
					// выйти, если нет лс для обработки
					break;
				}

				// Получить очередную пачку лицевых
				List<String> batch = new LinkedList<String>();

				if (setBestCntThread) {
					cntThread = bestCntThread;
				}
				for (int b =1; b <= cntThread ; b++) {
					String addLsk = qu.poll();
					if (addLsk ==null) {
						break;
					} else {
						batch.add(addLsk);
					}
				}

				long startTime2;
				long endTime2;
				startTime2 = System.currentTimeMillis();
				List<Future<Result>> frl = new ArrayList<Future<Result>>();

				// Начать потоки
				for (String lsk :batch){
					ComprTbl comprTbl = ctx.getBean(ComprTbl.class);
					Future<Result> fut = comprTbl.comprTableByLsk(table, lsk, backPeriod, curPeriod, isAllPeriods, isByUsl);
					frl.add(fut);
					if (cnt == 1000) {
						log.info("Последний лс на обработке={}", lsk);
						cnt = 1;
					} else {
						cnt ++;
					}
				}

				// проверить окончание всех потоков
				int flag2 = 0;
				while (flag2 == 0) {
					//log.info("========================================== Waiting for threads");
					flag2 = 1;
					for (Future<Result> fut : frl) {
						if (!fut.isDone()) {
							flag2 = 0;
						} else {
								try {
									if (fut.get().getErr() != 0) {
										throw new Exception("Ошибка в потоке err="+fut.get().getErr());
									}
									//log.info("Done thread Lsk={}, Result.err={}",
									//		fut.get().getLsk(), fut.get().getErr());
								} catch (InterruptedException | ExecutionException e) {
									// TODO Auto-generated catch block
									log.error(Utl.getStackTraceString(e));
								}
						}
					}

					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

				endTime2 = System.currentTimeMillis();
				batchTime = (int) ((endTime2 - startTime2) /cntThread);
				log.info("Выполнение одного лс используя {} потоков заняло {} мс", cntThread, batchTime);

				// проверить время выполнения пачки
				if (!setBestCntThread) {
					avgLst.add(batchTime);
					// пока не установлено лучшее кол-во потоков
					if (batchCnt > 5) {
						batchCnt = 1;

						IntSummaryStatistics stat = avgLst.stream().mapToInt((t) -> t)
                                .summaryStatistics();

						if (stat.getAverage() < bestTime) {
							bestCntThread = cntThread;
							bestTime = (int) stat.getAverage();
							log.info("Кол-во потоков={}, найдено лучшее время исполнения одного лс={}", bestCntThread, bestTime);
						}
						if (cntThread !=1) {
							cntThread--;
						} else {
							cntThread = bestCntThread;
							setBestCntThread = true;
							log.info("Установлено лучшее кол-во потоков={}", bestCntThread);
						}
						avgLst = new TreeList<Integer>();
					} else {
						batchCnt++;
					}
				}

		}

		endTime = (int) System.currentTimeMillis();
		totalTime = endTime - startTime;
		log.info("Overall time for compress:{} sec", totalTime/1000, 2);

	}

	/**
	 * Сжать все необходимые таблицы
	 * @param - firstLsk - начать с лиц.сч.
	 * @param - oneLsk - только этот лицевой
	 * @return
	 */
	@Override
	public boolean comprAllTables(String firstLsk, String oneLsk, String table, boolean isAllPeriods) {
		log.info("===Version 2.0.1===");
		this.isAllPeriods = isAllPeriods;
		// Получить параметры
		param = paramDao.findAll().stream().findFirst().orElse(null);
		curPeriod = Integer.valueOf(param.getPeriod());
    	// Период -2 от текущего (минус два месяца, так как сжимаем только архивный и сравниваем его с доархивным)
    	backPeriod = Integer.valueOf(Utl.addMonths(String.valueOf(curPeriod), -2));
		log.trace("Текущий период={}", curPeriod);
		log.trace("Период -2 от текущего={}", backPeriod);
		try {
			comprTable(table, firstLsk, oneLsk, true);
		} catch (Exception e) {
			// Ошибка при выполнении
			e.printStackTrace();
			return false;
		}
		return true;
	}





}
