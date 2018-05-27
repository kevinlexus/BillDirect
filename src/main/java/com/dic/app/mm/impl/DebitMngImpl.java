package com.dic.app.mm.impl;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.DebitMng;
import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.Config;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebUslDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.PenUslCorrDAO;
import com.dic.bill.dao.SprPenUslDAO;
import com.dic.bill.dao.StavrUslDAO;
import com.dic.bill.dao.VchangeDetDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.SumPenRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.DebPenUsl;
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
	private DebUslDAO debUslDao;
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
	private ThreadMng threadMng;
	@Autowired
	private DebitThrMng debitThrMng;

	@PersistenceContext
    private EntityManager em;


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
		// лиц.счет
		//calcStore.setLsk(lsk);
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

		// вызвать расчет задолженности и пени
		//genDebit(kart, calcStore);
		// получить список лицевых счетов
		List<String> lstItem = kartDao.getAll().stream().map(t->t.getLsk()).collect(Collectors.toList());
		// вызвать в потоках
		threadMng.invokeThreads(calcStore, 5, lstItem);

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("Расчет задолженности - ОКОНЧАНИЕ! Время расчета={}", totalTime);
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
		localStore.setLstDebFlow(debUslDao.getDebitByLsk(lsk, periodBack));
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

		//localStore.setLstFlow(lstFlow);

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
					throw new UncheckedIOException("ОШИБКА в процессе начисления пени по лc.="+lsk, null);
				}
			})
			.collect(Collectors.toList());

/*		if (1==1) {
			CommonResult res = new CommonResult(kart.getLsk(), 1111111111);
			return new AsyncResult<CommonResult>(res);
		}
*/

		log.info("del lsk={}, period={}", lsk, period);
		// удалить записи текущего периода
		debUslDao.delByLskPeriod(lsk, period);

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
						t.getSummaDeb(), t.getSummaRollDeb(), t.getPenyaIn(),
						t.getPenyaCur(), t.getPenyaCorr(), t.getPenyaOut(),
						t.getDays());
			}
			// сохранить задолжность
			DebPenUsl debPenUsl = DebPenUsl.builder()
								.withKart(kart)
								.withUsl(em.find(Usl.class, t.getUslOrg().getUslId()))
								.withOrg(em.find(Org.class, t.getUslOrg().getOrgId()))
								.withSumma(t.getSummaDeb())
								.withSummaRolled(t.getSummaRollDeb())
								.withPenyaCur(t.getPenyaCur())
								.withPenyaCorr(t.getPenyaCorr())
								.withPenya(t.getPenyaOut())
								.withDays(t.getDays())
								.withMg(t.getMg())
								.withPeriod(period)
								.build();
			em.persist(debPenUsl);
		});

		log.info("Расчет задолженности по лиц.счету - ОКОНЧАНИЕ!");


		CommonResult res = new CommonResult(kart.getLsk(), 1111111111);
		return new AsyncResult<CommonResult>(res);

	}

	/**
	 * Сгруппировать по периодам пеню, и долги на дату расчета
	 * @param uslOrg - услуга и организация
	 * @param lst - долги по всем дням
	 * @throws ErrorWhileChrgPen
	 */
	private List<SumPenRec> getGroupingPenDeb(UslOrg uslOrg, List<SumDebRec> lst) throws ErrorWhileChrgPen {
		// получить долги на последнюю дату
		List<SumPenRec> lstDebAmnt =  lst.stream()
				.filter(t-> t.getIsLastDay() == true)
				.map(t-> new SumPenRec(uslOrg, t.getSummaDeb(), t.getSummaRollDeb(), t.getPenyaIn(),
						t.getPenyaCorr(), t.getDays(), t.getMg()))
				.collect(Collectors.toList());

		// сгруппировать начисленную пеню по периодам
		for (SumDebRec t :lst) {
			addPen(uslOrg, lstDebAmnt, t.getMg(), t.getPenyaCur(), t.getDays());
		}
		lstDebAmnt.forEach(t-> {
			// округлить начисленную пеню до копеек, сохранить, добавить корректировки
			t.setPenyaCur(t.getPenyaCur().setScale(2, RoundingMode.HALF_UP).add(t.getPenyaCorr()));
			// установить исходящее сальдо
			t.setPenyaOut(t.getPenyaIn().add(t.getPenyaCur()));
		});

		return lstDebAmnt;
	}

	/**
	 * добавить пеню по периоду в долги по последней дате
	 * @param uslOrg - услуга и организация
	 * @param lstDebAmnt - коллекция долгов
	 * @param mg - период долга
	 * @param penya - начисленая пеня за день
	 * @param days - дней просрочки (если не будет найден период в долгах, использовать данный параметр)
	 * @throws ErrorWhileChrgPen
	 */
	private void addPen(UslOrg uslOrg, List<SumPenRec> lstDebAmnt, Integer mg, BigDecimal penya, Integer days) throws ErrorWhileChrgPen {
		// найти запись долга с данным периодом
		SumPenRec recDeb = lstDebAmnt.stream()
				.filter(t-> t.getMg().equals(mg)).findFirst().orElse(null);
		if (recDeb != null) {
			// запись найдена, сохранить значение пени
			recDeb.setPenyaCur(recDeb.getPenyaCur().add(penya));
		} else {
			// запись НЕ найдена, создать новую, сохранить значение пени
			// вообще, должна быть найдена запись, иначе, ошибка в коде!
			throw new ErrorWhileChrgPen("Не найдена запись долга в процессе сохранения значения пени!");
		}
	}

}