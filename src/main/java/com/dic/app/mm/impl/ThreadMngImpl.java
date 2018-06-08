package com.dic.app.mm.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.CommonResult;
import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис создания потоков
 * @author Lev
 * @version 1.00
 */
@Slf4j
@Service
public class ThreadMngImpl<T> implements ThreadMng<T> {

	@Autowired
	private ApplicationContext ctx;
    @PersistenceContext
    private EntityManager em;
	@Autowired
	private ConfigApp config;


	/**
	 * Вызвать выполнение потоков распределения объемов/ начисления
	 * @param reqConfig - конфиг запроса
	 * @param cntThreads - кол-во потоков
	 * @param lstItem - список Id на обработку
	 * @param isCheckStop - проверять маркер остановки процесса?
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Override
	public void invokeThreads(PrepThread<T> reverse, CalcStore calcStore,
			int cntThreads, List<T> lstItem, boolean isCheckStop) throws InterruptedException, ExecutionException {
		long startTime = System.currentTimeMillis();

		List<Future<CommonResult>> frl = new ArrayList<Future<CommonResult>>(cntThreads);
		for (int i=1; i<=cntThreads; i++) {
			frl.add(null);
		}
		// проверить окончание всех потоков и запуск новых потоков
		T itemWork = null;
		boolean isStop = false;
		// флаг принудительной остановки
		boolean isStopProcess = false;
		while (!isStop && !isStopProcess) {
			//log.info("========================================== Ожидание выполнения потоков ===========");
			Future<CommonResult> fut;
			int i=0;
			// флаг наличия потоков
			isStop = true;
			for (Iterator<Future<CommonResult>> itr = frl.iterator(); itr.hasNext();) {
				if (isCheckStop && config.getLock().isStopped("debitMng.genDebitAll")) {
					// если процесс был остановлен, выход
					isStopProcess = true;
					break;
				}

				fut = itr.next();
				if (fut == null) {
					// получить новый объект
					itemWork = getNextItem(lstItem);
					if (itemWork != null) {
						// создать новый поток
						fut = reverse.myStringFunction(itemWork);
						//log.info("================================ Начат поток начисления для лс={} ==================", itemWork);
						frl.set(i, fut);
					}
				} else if (!fut.isDone()) {
					//log.info("========= Поток НЕ завершен! лс={}", fut.get().getLsk());
					//log.info("..................................... CHK1");
				} else {
					//log.info("------------------------------------- CHK2");
					//try {
						if (fut.get().getErr() == 1) {
							log.error("================================ ОШИБКА ПОЛУЧЕНА ПОСЛЕ ЗАВЕРШЕНИЯ ПОТОКА для лс={} ==================", fut.get().getErr());
						} else {
							//log.info("================================ Успешно завершен поток начисления для лс={} ==================", fut.get().getLsk());
						}
					/*} catch (InterruptedException | ExecutionException e1) {
						e1.printStackTrace();
						log.error("ОШИБКА ВО ВРЕМЯ ВЫПОЛНЕНИЯ ПОТОКА!", e1);
					} finally {*/
						// очистить переменную потока
						frl.set(i, null);
					//}

				}

				if (fut !=null) {
					// не завершен поток
					isStop = false;
				}
				i++;
			}

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.error(Utl.getStackTraceString(e));
			}
		}
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		if (lstItem.size() > 0) {
			log.info("Итоговое время выполнения одного {} cnt={}, мс."
					, totalTime / lstItem.size());
		}
	}

	// получить следующий объект, для расчета в потоках
	private T getNextItem(List<T> lstItem) {
		Iterator<T> itr = lstItem.iterator();
		T item = null;
		if (itr.hasNext()) {
			item  = itr.next();
			itr.remove();
		}

		return item;
	}




}