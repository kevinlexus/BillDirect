package com.dic.app.mm.impl;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenMng;
import com.dic.app.mm.GenThrMng;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.SprGenItm;
import com.ric.cmn.excp.ErrorWhileGen;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope("prototype")
public class GenThrMngImpl implements GenThrMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private ConfigApp config;
	@Autowired
	private SprGenItmDAO sprGenItemDao;
	@Autowired
	private GenMng genMng;

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
		// прогресс - 0
		config.setProgress(0);

		SprGenItm menuGenItg = sprGenItemDao.getByCd("GEN_ITG");
		SprGenItm menuMonthOver = sprGenItemDao.getByCd("GEN_MONTH_OVER");
		SprGenItm menuCheckBG = sprGenItemDao.getByCd("GEN_CHECK_BEFORE_GEN");

		//**********почистить ошибку последнего формирования, % выполнения
		genMng.clearError(menuGenItg);

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
				// статус - формирование остановлено
				config.setStateGen("0");
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
				// статус - формирование остановлено
				config.setStateGen("0");
				return;
			}
			log.info("Проверки до перехода месяца выполнены!");
		}











		while (config.getStateGen().equals("1")) {
			try {
				Thread.sleep(1000);
				config.setProgress(config.getProgress()+1);
				log.info("ПРОВЕРКА ПОТОКА!");
			} catch (InterruptedException e) {
				e.printStackTrace();

			}

		}


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