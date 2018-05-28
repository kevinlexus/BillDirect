package com.dic.app.mm.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

import com.dic.app.mm.DebitMng;
import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.Config;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebPenUslDAO;
import com.dic.bill.dao.DebPenUslTempDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.PenUslCorrDAO;
import com.dic.bill.dao.SprPenUslDAO;
import com.dic.bill.dao.StavrUslDAO;
import com.dic.bill.dao.VchangeDetDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumPenRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.DebPenUsl;
import com.dic.bill.model.scott.DebPenUslTemp;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.CommonResult;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope("prototype")
public class DebitMngImpl implements DebitMng {

	@Autowired
	private Config config;
	@Autowired
	private DebPenUslDAO debPenUslDao;
	@Autowired
	private DebPenUslTempDAO debPenUslTempDao;
	@Autowired
	private ChargeDAO chargeDao;
	@Autowired
	private VchangeDetDAO vchangeDetDao;
	@Autowired
	private KwtpDayDAO kwtpDayDao;
	@Autowired
	private CorrectPayDAO correctPayDao;
	@Autowired
	private SprPenUslDAO sprPenUslDao;
	@Autowired
	private StavrUslDAO stavrUslDao;
	@Autowired
	private PenUslCorrDAO penUslCorrDao;
	@Autowired
	private KartDAO kartDao;
	@Autowired
	private ThreadMng<String> threadMng;
	@Autowired
	private DebitThrMng debitThrMng;
	@Autowired
	private ApplicationContext ctx;

	@PersistenceContext
    private EntityManager em;

	// Метод лямбда, для выполнения внутри сервиса потоков
	public static Future<CommonResult> reverseStr(PrepThread<String> reverse, String lsk){
		  return reverse.myStringFunction(lsk);
	}

	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет, если отсутствует - весь фонд
	 * @param genDt - дата расчета
	 * @param debugLvl - уровень отладочной информации (0-нет, 1-отобразить)
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
	public void genDebitAll(String lsk, Date genDt, Integer debugLvl) {
		long startTime = System.currentTimeMillis();
		log.info("Расчет задолженности - НАЧАЛО!");

		// загрузить справочники
		CalcStore calcStore = new CalcStore();
		// уровень отладки
		calcStore.setDebugLvl(debugLvl);
		// начальная дата расчета
		calcStore.setDt1(config.getCurDt1());
		// дата расчета пени
		calcStore.setGenDt(genDt);
		// текущий период
		calcStore.setPeriod(Integer.valueOf(config.getPeriod()));
		// период - месяц назад
		calcStore.setPeriodBack(Integer.valueOf(config.getPeriodBack()));
		// справочник дат начала пени
		calcStore.setLstSprPenUsl(sprPenUslDao.findAll());
		// справочник ставок рефинансирования
		calcStore.setLstStavrUsl(stavrUslDao.findAll());

		calcStore.getLstSprPenUsl().size();
		calcStore.getLstStavrUsl().size();

		// получить список лицевых счетов
		List<String> lstItem;
		lstItem = kartDao.getAll()
				.stream().map(t->t.getLsk()).collect(Collectors.toList());

		if (lsk == null) {
			// по всем лиц.счетам
			lstItem = kartDao.getAll()
					.stream().map(t->t.getLsk()).collect(Collectors.toList());
 		} else {
 			// по одному лиц.счету
 			lstItem = new ArrayList<String>(0);
 			lstItem.add(lsk);
 		}

		// будет выполнено позже, в создании потока
		PrepThread<String> reverse = (item) -> {
			// сервис расчета задолженности и пени
			DebitMng debitMng = ctx.getBean(DebitMng.class);
			return debitMng.genDebit(item, calcStore);
		};

		// вызвать в потоках
		threadMng.invokeThreads(reverse, calcStore, 1, lstItem);

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("Расчет задолженности - ОКОНЧАНИЕ! Общее время расчета={}", totalTime);
	}


	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет
	 * @param calcStore - хранилище справочников
	 * @param genDt - дата расчета
	 */
	@Async
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
	public Future<CommonResult> genDebit(String lsk, CalcStore calcStore) {
		long startTime = System.currentTimeMillis();
		log.info("Расчет задолженности по лиц.счету - НАЧАЛО!");
		Kart kart = em.find(Kart.class, lsk);
		//String lsk = kart.getLsk();
		// текущий период
		Integer period = calcStore.getPeriod();
		// период - месяц назад
		Integer periodBack = calcStore.getPeriodBack();

		CalcStoreLocal localStore = new CalcStoreLocal();
		// ЗАГРУЗИТЬ все финансовые операции по лиц.счету
		// задолженность предыдущего периода (здесь же заполнены поля по вх.сальдо по пене) - 1
		localStore.setLstDebFlow(debPenUslDao.getDebitByLsk(lsk, periodBack));
		// текущее начисление - 2
		localStore.setLstChrgFlow(chargeDao.getChargeByLsk(lsk));
		// перерасчеты - 5
		localStore.setLstChngFlow(vchangeDetDao.getVchangeDetByLsk(lsk));
		// оплата долга - 3
		localStore.setLstPayFlow(kwtpDayDao.getKwtpDaySumByLsk(lsk));
		// оплата пени - 4
		localStore.setLstPayPenFlow(kwtpDayDao.getKwtpDayPenByLsk(lsk));
		// корректировки оплаты - 6
		localStore.setLstPayCorrFlow(correctPayDao.getCorrectPayByLsk(lsk));
		// корректировки начисления пени - 7
		localStore.setLstPenChrgCorrFlow(penUslCorrDao.getPenUslCorrByLsk(lsk));

		// создать список уникальных элементов услуга+организация
		localStore.createUniqUslOrg();
		// получить список уникальных элементов услуга+организация
		List<UslOrg> lstUslOrg = localStore.getUniqUslOrg();

		// обработать каждый элемент услуга+организация
		List<SumPenRec> lst = null;

		lst = lstUslOrg.parallelStream()
			//.filter(t-> t.getUslId().equals("005") && t.getOrgId().equals(10))
			.flatMap(t -> {
				try {
					// РАСЧЕТ задолжности и пени по услуге
					return debitThrMng.genDebitUsl(kart, t, calcStore, localStore).stream();
				} catch (ErrorWhileChrgPen e) {
					e.printStackTrace();
					throw new RuntimeException("ОШИБКА в процессе начисления пени по лc.="+lsk);
				}
			})
			.collect(Collectors.toList());

		// удалить записи текущего периода
		debPenUslDao.delByLskPeriod(lsk, period);
		debPenUslTempDao.delByIter(11111111); //TODO

		// СОХРАНИТЬ расчет
		if (calcStore.getDebugLvl().equals(1)) {
			log.info("");
			log.info("ИТОГОВАЯ задолжность и пеня");
		}


		lst.forEach(t-> {
			if (calcStore.getDebugLvl().equals(1)) {
				log.info("uslId={}, orgId={}, период={}, долг={}, свернутый долг={}, "
						+ "пеня вх.={}, пеня тек.={} руб., корр.пени={}, пеня исх.={}, дней просрочки(на дату расчета)={}",
						t.getUslOrg().getUslId(), t.getUslOrg().getOrgId(), t.getMg(),
						t.getDebOut(), t.getDebRolled(), t.getPenyaIn(),
						t.getPenyaChrg(), t.getPenyaCorr(), t.getPenyaOut(),
						t.getDays());
			}
			// сохранить задолжность
			DebPenUsl debPenUsl = DebPenUsl.builder()
								.withKart(kart)
								.withUsl(em.find(Usl.class, t.getUslOrg().getUslId()))
								.withOrg(em.find(Org.class, t.getUslOrg().getOrgId()))
								.withDebOut(t.getDebOut())
								.withPenOut(t.getPenyaOut())
								.withMg(t.getMg())
								.withPeriod(period)
								.build();
			em.persist(debPenUsl);

			// сохранить задолжность для отчета
			DebPenUslTemp debPenUslTemp = DebPenUslTemp.builder()
					.withUsl(em.find(Usl.class, t.getUslOrg().getUslId()))
					.withOrg(em.find(Org.class, t.getUslOrg().getOrgId()))
					.withDebIn(t.getDebIn())
					.withDebOut(t.getDebOut())
					.withDebRolled(t.getDebRolled())
					.withChrg(t.getChrg())
					.withChng(t.getChng())
					.withDebPay(t.getDebPay())
					.withPayCorr(t.getPayCorr())
					.withPenIn(t.getPenyaIn())
					.withPenOut(t.getPenyaOut())
					.withPenChrg(t.getPenyaChrg())
					.withPenCorr(t.getPenyaCorr())
					.withPenPay(t.getPenyaPay())
					.withDays(t.getDays())

					.withMg(t.getMg())
					.withIter(11111111) //TODO
					.build();
			em.persist(debPenUslTemp);
		});



		CommonResult res = new CommonResult(kart.getLsk(), 1111111111);
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("Расчет задолженности по лиц.счету - ОКОНЧАНИЕ! Время расчета={} мс", totalTime);
		return new AsyncResult<CommonResult>(res);

	}


}