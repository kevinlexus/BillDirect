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
	private SprGenItmDAO sprGenItmDao;
	@Autowired
	private VvodDAO vvodDao;
	@Autowired
	private HouseDAO houseDao;
	@Autowired
	private ExecMng execMng;
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
		// маркер формирования
		config.getLock().setLockProc(1, "MainGeneration");

		try {
		// прогресс - 0
		config.setProgress(0);

		SprGenItm menuGenItg = sprGenItmDao.getByCd("GEN_ITG");
		SprGenItm menuMonthOver = sprGenItmDao.getByCd("GEN_MONTH_OVER");
		SprGenItm menuCheckBG = sprGenItmDao.getByCd("GEN_CHECK_BEFORE_GEN");

		//**********почистить ошибку последнего формирования, % выполнения
		//genMng.clearError(menuGenItg);

		//**********установить дату формирования
		execMng.setGenDate();

		//**********Закрыть базу для пользователей, если выбрано итоговое формир
		if (menuGenItg.getSel()) {
			execMng.stateBase(1);
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
			execMng.setPercent(menuCheckBG, 1);
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
			execMng.setPercent(menuMonthOver, 1);
			log.info("Проверки до перехода месяца выполнены!");
		}
		execMng.setPercent(menuGenItg, 0.10D);

		// список Id объектов
		List<Integer> lst;
		//**********Начать формирование
		for (SprGenItm itm : sprGenItmDao.getAllCheckedOrdered()) {

				log.info("Generating menu item: {}", itm.getCd());

				switch (itm.getCd()) {

				case "GEN_ADVANCE":
					// переформировать авансовые платежи
					execMng.execProc(36, null, null);
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.20D);
					break;

				case "GEN_DIST_VOLS1":
					//чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
					execMng.execProc(17, null, null);
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.25D);
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
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.40D);
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
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.45D);
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
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.50D);
					break;

				case "GEN_SAL":
					//сальдо по лиц счетам
					execMng.execProc(19, null, null);
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.65D);
					break;
				case "GEN_FLOW":
					// движение
					execMng.execProc(20, null, null);
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.70D);
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
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.75D);
					break;
				case "GEN_PENYA_DIST": {
					// распределение пени по исх сальдо
					execMng.execProc(21, null, null);
					// проверить распр.пени
					Integer ret = execMng.execProc(13, null, null);
					if (ret.equals(1)) {
						// найдены ошибки - выход
						menuGenItg.setState("Найдены ошибки в процессе проверки распределения пени!");
						log.info("Найдены ошибки в процессе проверки распределения пени!");
						return;
					}
					execMng.setPercent(itm, 1);
					execMng.setPercent(menuGenItg, 0.80D);
					break;
				}

				}
		}

		// выполнено всё
		execMng.setPercent(menuGenItg, 1D);

	}	finally {
		// формирование остановлено
		// снять маркер выполнения
		config.getLock().unlockProc(1, "MainGeneration");
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
			//execMng.setPercent(spr, proc);
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
		Integer ret = execMng.execProc(38, null, null);
		boolean isErr = false;
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(4, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(5, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(6, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(7, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		ret = execMng.execProc(12, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		ret = execMng.execProc(8, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		ret = execMng.execProc(15, null, null);
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
		Integer ret = execMng.execProc(8, null, null);
		boolean isErr = false;
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(10, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(11, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}
		ret = execMng.execProc(14, null, null);
		if (ret.equals(1)) {
			// установить статус ошибки, выйти из формирования
			isErr = true;
		}

		return isErr;
	}

}