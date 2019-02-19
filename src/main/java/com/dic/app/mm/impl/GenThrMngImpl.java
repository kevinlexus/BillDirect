package com.dic.app.mm.impl;

import java.util.concurrent.Future;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ExecMng;
import com.dic.app.mm.GenThrMng;
import com.dic.bill.model.scott.SprGenItm;
import com.ric.dto.CommonResult;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope("prototype")
public class GenThrMngImpl implements GenThrMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private ExecMng execMng;

	/**
	 * Выполнить поток. Propagation.REQUIRES_NEW - так как не удается в новом потоке
	 * продолжить транзакцию от главного, REQUIRED - не помогло.
	 *
	 * @param var - вид задачи
	 * @param id - id объекта
	 */
	@Async
	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor=Exception.class)
	public Future<CommonResult> doJob(Integer var, Long id, SprGenItm spr, double proc) {
		switch (var) {
		case 1:
			log.info("Выполняется поток распределения объемов во вводе id={}, где нет ОДПУ", id);
			execMng.execProc(100, id, null);
			log.info("Выполнился поток распределения объемов во вводе id={}, где нет ОДПУ", id);
			break;
		case 2:
			log.info("Выполняется поток распределения объемов во вводе id={}, где есть ОДПУ", id);
			execMng.execProc(101, id, null);
			log.info("Выполнился поток распределения объемов во вводе id={}, где есть ОДПУ", id);
			break;
		case 3:
			//**********установить дату формирования, так как новая транзакция (Propagation.REQUIRES_NEW)
			execMng.setGenDate();

			log.info("Выполняется поток начисления пени по дому c_house.id={}", id);
			execMng.execProc(102, id, null);
			log.info("Выполнился поток начисления пени по дому c_house.id={}", id);
			break;
		case 4:
			log.info("Выполняется поток расчета начисления по дому c_house.id={}", id);
			execMng.execProc(103, id, null);
			log.info("Выполнился поток расчета начисления по дому c_house.id={}", id);
			break;

		}
		// сохранить процент выполнения
		execMng.setMenuElemPercent(spr, proc);

		CommonResult res = new CommonResult(0, 0);
		return new AsyncResult<>(res);
	}
}