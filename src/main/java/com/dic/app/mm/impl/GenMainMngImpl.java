package com.dic.app.mm.impl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.ExecMng;
import com.dic.app.mm.GenMainMng;
import com.dic.app.mm.GenThrMng;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.dao.HouseDAO;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.dao.VvodDAO;
import com.dic.bill.model.scott.SprGenItm;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope("prototype")
public class GenMainMngImpl implements GenMainMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private ConfigApp config;
	@Autowired
	private SprGenItmDAO sprGenItemDao;
	@Autowired
	private VvodDAO vvodDao;
	@Autowired
	private HouseDAO houseDao;
	@Autowired
	private ExecMng genMng;
	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private ThreadMng<Integer> threadMng;

	/**
	 * ОСНОВНОЙ поток формирования
	 * @throws ErrorWhileGen
	 */
	@Override
	@Async
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public void startMainThread() {

		// установить статус - формирование
		config.setStateGen("1");
		config.getLock().setLockProc(1, "MainGeneration");

		try {
		// прогресс - 0
		config.setProgress(0);

		SprGenItm menuGenItg = sprGenItemDao.getByCd("GEN_ITG");
		SprGenItm menuMonthOver = sprGenItemDao.getByCd("GEN_MONTH_OVER");
		SprGenItm menuCheckBG = sprGenItemDao.getByCd("GEN_CHECK_BEFORE_GEN");

		//**********почистить ошибку последнего формирования, % выполнения
		//genMng.clearError(menuGenItg);

		//**********установить дату формирования
		genMng.setGenDate();

		//**********Закрыть базу для пользователей, если выбрано итоговое формир
		if (menuGenItg.getSel()) {
			genMng.stateBase(1);
			log.info("Установлен признак закрытия базы!");
		}

		//**********Проверки до формирования
		if (menuCheckBG.getSel()) {
			// если выбраны проверки, а они как правило д.б. выбраны при итоговом
			if (checkErr()) {
				// найдены ошибки - выход
				menuGenItg.setState("Найдены ошибки до формирования!");
				log.info("Найдены ошибки до формирования!");
				return;
			}
			log.info("Проверки до формирования выполнены!");
		}

		//**********Проверки до перехода месяца
		if (menuMonthOver.getSel()) {
			if (checkMonthOverErr()) {
				// найдены ошибки - выход
				menuGenItg.setState("Найдены ошибки до перехода месяца!");
				log.info("Найдены ошибки до перехода месяца!");
				return;
			}
			log.info("Проверки до перехода месяца выполнены!");
		}

		// список Id объектов
		List<Integer> lst;

		//**********Начать формирование
		for (SprGenItm itm : sprGenItemDao.getAllCheckedOrdered()) {
				log.info("Generating menu item: " + itm.getCd());
				switch (itm.getCd()) {

				case "GEN_ADVANCE":
					// переформировать авансовые платежи
					genMng.execProc(36, null, null);
					break;

				case "GEN_DIST_VOLS1":
					//чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
					genMng.execProc(17, null, null);
					break;
				case "GEN_DIST_VOLS2":
					// распределить где нет ОДПУ
					lst = vvodDao.getWoODPU()
						.stream().map(t->t.getId()).collect(Collectors.toList());
					if (!doInThread(lst, itm, 1)) {
						// ошибка распределения
						menuGenItg.setState("Найдены ошибки во время распределения объемов где нет ОДПУ!");
						log.info("Найдены ошибки во время распределения объемов где нет ОДПУ!");
						return;
					}
					break;
				case "GEN_DIST_VOLS3":
					//распределить где есть ОДПУ
					lst = vvodDao.getWithODPU()
						.stream().map(t->t.getId()).collect(Collectors.toList());
					if (!doInThread(lst, itm, 2)) {
						// ошибка распределения
						menuGenItg.setState("Найдены ошибки во время распределения объемов где есть ОДПУ!");
						log.info("Найдены ошибки во время распределения объемов где есть ОДПУ!");
						return;
					}
					break;

				case "GEN_CHRG":
					// начисление по домам
					lst = houseDao.findAll()
					.stream().map(t->t.getId()).collect(Collectors.toList());
					if (!doInThread(lst, itm, 4)) {
						// ошибка распределения
						menuGenItg.setState("Найдены ошибки во время расчета начисления по домам!");
						log.info("Найдены ошибки во время расчета начисления домам!");
						return;
					}
					break;

				case "GEN_PENYA":
					// начисление пени по домам
					lst = houseDao.getNotClosed()
						.stream().map(t->t.getId()).collect(Collectors.toList());
					if (!doInThread(lst, itm, 3)) {
						// ошибка распределения
						menuGenItg.setState("Найдены ошибки во время начисления пени по домам!");
						log.info("Найдены ошибки во время начисления пени по домам!");
						return;
					}

				}

		}






			try {
				Thread.sleep(3000);
				config.setProgress(config.getProgress()+1);
				log.info("ПРОВЕРКА ПОТОКА!");
			} catch (InterruptedException e) {
				e.printStackTrace();

			}



	}	finally {
		// снять маркер выполнения
		config.getLock().unlockProc(1, "MainGeneration");
		// статус - формирование остановлено
		config.setStateGen("0");
	}

	}

	/**
	 * Выполнение в потоках
	 * @param lst - список Id вводов
	 * @param spr - элемент меню
	 * @param var - вариант выполнения
	 * @return
	 */
	private boolean doInThread(List<Integer> lst, SprGenItm spr, int var) {
		// будет выполнено позже, в создании потока
		PrepThread<Integer> reverse = (item, proc) -> {
			// сохранить процент выполнения
			genMng.setProc(spr, proc);
			// потоковый сервис
			GenThrMng genThrMng = ctx.getBean(GenThrMng.class);
			return genThrMng.doJob(var, item, spr, proc);
		};

		// вызвать в потоках
		try {
			// вызвать потоки, проверять наличие маркера работы процесса
			threadMng.invokeThreads(reverse, 1, lst, "MainGeneration");
		} catch (InterruptedException | ExecutionException e) {
			log.error(Utl.getStackTraceString(e));
			return false;
		}
		return true;
	}

	/**
	 * Проверка ошибок
	 * вернуть false - если нет ошибок
	 */
	private boolean checkErr() {
		// новая проверка, на список домов в разных УК, по которым обнаружены открытые лицевые счета
		Integer ret = genMng.execProc(38, null, null);
		boolean isErr = false;
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(4, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(5, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(6, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(7, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		ret = genMng.execProc(12, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		ret = genMng.execProc(8, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		ret = genMng.execProc(15, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		return isErr;
	}

	/**
	 * Проверка ошибок до перехода
	 * вернуть false - если нет ошибок
	 */
	private boolean checkMonthOverErr() {
		// новая проверка, на список домов в разных УК, по которым обнаружены открытые лицевые счета
		Integer ret = genMng.execProc(8, null, null);
		boolean isErr = false;
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(10, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(11, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = genMng.execProc(14, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		return isErr;
	}

}