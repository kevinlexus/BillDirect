package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

import com.dic.app.mm.DebitMng;
import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ReferenceMng;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.Config;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.PenDAO;
import com.dic.bill.dao.PenUslCorrDAO;
import com.dic.bill.dao.SprPenUslDAO;
import com.dic.bill.dao.StavrUslDAO;
import com.dic.bill.dao.VchangeDetDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumDebPenRec;
import com.dic.bill.dto.SumPenRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Deb;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Pen;
import com.dic.bill.model.scott.SessionDirect;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.CommonResult;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope("prototype")
public class DebitMngImpl implements DebitMng {

	@Autowired
	private Config config;
	@Autowired
	private DebDAO debDao;
	@Autowired
	private PenDAO penDao;
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
	private ReferenceMng refMng;
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
	 * @param iter - номер итерации расчета (чтобы потом выбрать из таблицы для отчета)
	 * @param sessionId - Id сессии
	 * @throws ErrorWhileChrgPen
	 */
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor=Exception.class)
	public void genDebitAll(String lsk, Date genDt, Integer debugLvl, SessionDirect sessionDirect) throws ErrorWhileChrgPen {
		long startTime = System.currentTimeMillis();
		log.info("НАЧАЛО расчета задолженности");

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
		// тип выполнения (0 - по одному лс, вывести отчет в C_DEBPEN_USL_TEMP,
		//				   1 - вывести исх сальдо)
		int tp;
		if (lsk == null) {
			// по всем лиц.счетам
			tp = 0;
			lstItem = kartDao.getAll()
					.stream().map(t->t.getLsk()).collect(Collectors.toList());
 		} else {
 			// по одному лиц.счету
			tp = 1;
 			lstItem = new ArrayList<String>(0);
 			lstItem.add(lsk);
 		}

		// будет выполнено позже, в создании потока
		PrepThread<String> reverse = (item) -> {
			// сервис расчета задолженности и пени
			DebitMng debitMng = ctx.getBean(DebitMng.class);
			return debitMng.genDebit(item, calcStore, tp, sessionDirect);
		};

		// вызвать в потоках
		try {
			threadMng.invokeThreads(reverse, calcStore, 5, lstItem);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new ErrorWhileChrgPen("ОШИБКА во время расчета задолженности и пени!");
		}

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("ОКОНЧАНИЕ расчета задолженности - Общее время расчета={}", totalTime);
	}


	/**
	 * Расчет задолжности и пени
	 * @param lsk - лиц.счет
	 * @param calcStore - хранилище справочников
	 * @param genDt - дата расчета
	 * @param tp - тип вызова:  0 - создание выходного отчета в C_DEBPEN_USL_TEMP
	 * 							1 - запись исходящего сальдо в C_DEBPEN_USL
	 * @param sessionId - Id сессии
	 */
	@Async
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW, rollbackFor=Exception.class)
	public Future<CommonResult> genDebit(String lsk, CalcStore calcStore, Integer tp, SessionDirect sessionDirect) {
		long startTime = System.currentTimeMillis();
		log.info("НАЧАЛО расчета задолженности по лиц.счету {}", lsk);
		Kart kart = em.find(Kart.class, lsk);
		//String lsk = kart.getLsk();
		// текущий период
		Integer period = calcStore.getPeriod();
		// период - месяц назад
		Integer periodBack = calcStore.getPeriodBack();

		CalcStoreLocal localStore = new CalcStoreLocal();
		// ЗАГРУЗИТЬ все финансовые операции по лиц.счету
		// задолженность предыдущего периода (здесь же заполнены поля по вх.сальдо по пене) - 1
		localStore.setLstDebFlow(debDao.getDebitByLsk(lsk, periodBack));

/*		penDao.getPenByLsk(lsk, periodBack).stream()
		.forEach(t->{
					log.info("ВХОДЯЩЕЕ сальдо по пене mg={}, usl={} org={}, сумма={}", t.getMg(), t.getUslId(), t.getOrgId(), t.getPenOut());

				});

*/		// задолженность по пене (вх.сальдо) - 8
		localStore.setLstDebPenFlow(penDao.getPenByLsk(lsk, periodBack));
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

		// СОХРАНИТЬ расчет
		if (calcStore.getDebugLvl().equals(1)) {
			log.info("");
			log.info("ИТОГОВАЯ задолжность и пеня");
		}


		// перенаправить пеню на услугу и организацию по справочнику REDIR_PAY
		redirectPen(kart, lst);

		// удалить записи текущего периода, если они были созданы
		debDao.delByLskPeriod(lsk, period);
		penDao.delByLskPeriod(lsk, period);
		// обновить mgTo записей, если они были расширены до текущего периода
		debDao.updByLskPeriod(lsk, period, periodBack);
		penDao.updByLskPeriod(lsk, period, periodBack);

		for (SumPenRec t : lst) {
				// сохранить расчет
				save(calcStore, kart, localStore, t);
		}


		CommonResult res = new CommonResult(kart.getLsk(), 1111111111); // TODO 111111
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		log.info("ОКОНЧАНИЕ расчета задолженности по лиц.счету {}! Время расчета={} мс", lsk, totalTime);
		return new AsyncResult<CommonResult>(res);

	}

	/**
	 * Сохранить расчет
	 * @param calcStore - хранилище справочников
	 * @param kart - лиц.счет
	 * @param localStore - локальное хранилище финансовых операций по лиц.счету
	 * @param t - рассчитанная строка
	 */
	private void save(CalcStore calcStore, Kart kart, CalcStoreLocal localStore, SumPenRec t) {
		BigDecimal penChrgRound = t.getPenyaChrg().setScale(2, RoundingMode.HALF_UP); // округлить начисленную пеню
		BigDecimal penyaOut =  t.getPenyaIn().add(penChrgRound).add(t.getPenyaCorr() 							   // прибавить корректировки
						).subtract(t.getPenyaPay() 				   // отнять оплату
								);
		// флаг создания новой записи
		boolean isCreate = false;
		if (calcStore.getDebugLvl().equals(1)) {
			log.info("uslId={}, orgId={}, период={}, долг={}, свернутый долг={}, "
					+ "пеня вх.={}, пеня тек.={} руб., корр.пени={}, пеня исх.={}, дней просрочки(на дату расчета)={}",
					t.getUslOrg().getUslId(), t.getUslOrg().getOrgId(), t.getMg(),
					t.getDebOut(), t.getDebRolled(), t.getPenyaIn(),
					t.getPenyaChrg(), t.getPenyaCorr(), penyaOut,
					t.getDays());
		}

		// найти запись долгов предыдущего периода
		SumDebPenRec foundDeb = localStore.getLstDebFlow().stream()
			.filter(d-> d.getUslId().equals(t.getUslOrg().getUslId()))
			.filter(d-> d.getOrgId().equals(t.getUslOrg().getOrgId()))
			.filter(d-> d.getMg().equals(t.getMg()))
			.findFirst().orElse(null);
		if (foundDeb == null) {
			// не найдена, создать новую запись
			isCreate = true;
		} else {
			// найдена, проверить равенство по полям
			if (Utl.isEqual(t.getDebIn(), foundDeb.getDebIn())
				&& Utl.isEqual(t.getDebOut(), foundDeb.getDebOut())
				&& Utl.isEqual(t.getDebRolled(), foundDeb.getDebRolled())
				&& Utl.isEqual(t.getChrg(), foundDeb.getChrg())
				&& Utl.isEqual(t.getChng(), foundDeb.getChng())
				&& Utl.isEqual(t.getDebPay(), foundDeb.getDebPay())
				&& Utl.isEqual(t.getPayCorr(), foundDeb.getPayCorr())
					) {
				// равны, расширить период
				//log.info("найти id={}", foundDeb.getId());
				Deb deb = em.find(Deb.class, foundDeb.getId());
				//log.info("найти deb={}", deb);
				deb.setMgTo(calcStore.getPeriod());
			} else {
				// не равны, создать запись нового периода
				isCreate = true;
			}
		}

		if (isCreate) {
			// создать запись нового периода
			Deb deb = Deb.builder()
					.withUsl(em.find(Usl.class, t.getUslOrg().getUslId()))
					.withOrg(em.find(Org.class, t.getUslOrg().getOrgId()))
					.withDebIn(t.getDebIn())
					.withDebOut(t.getDebOut())
					.withDebRolled(t.getDebRolled())
					.withChrg(t.getChrg())
					.withChng(t.getChng())
					.withDebPay(t.getDebPay())
					.withPayCorr(t.getPayCorr())
					.withKart(kart)
					.withMgFrom(calcStore.getPeriod())
					.withMgTo(calcStore.getPeriod())
					.withMg(t.getMg())
					.build();
			em.persist(deb);
		}
		// сбросить флаг создания новой записи
		isCreate = false;

		// найти запись пени предыдущего периода
		SumDebPenRec foundPen = localStore.getLstDebPenFlow().stream()
			.filter(d-> d.getUslId().equals(t.getUslOrg().getUslId()))
			.filter(d-> d.getOrgId().equals(t.getUslOrg().getOrgId()))
			.filter(d-> d.getMg().equals(t.getMg()))
			.findFirst().orElse(null);
		if (foundPen == null) {
			// не найдена, создать новую запись
			isCreate = true;
		} else {
			// найдена, проверить равенство по полям
			if (Utl.isEqual(t.getPenyaIn(), foundPen.getPenIn())
				&& Utl.isEqual(penyaOut, foundPen.getPenOut())
				&& Utl.isEqual(penChrgRound, foundPen.getPenChrg())
				&& Utl.isEqual(t.getPenyaCorr(), foundPen.getPenCorr())
				&& Utl.isEqual(t.getPenyaPay(), foundPen.getPenPay())
				&& Utl.isEqual(t.getDays(), foundPen.getDays())
					) {
				// равны, расширить период
				Pen pen = em.find(Pen.class, foundPen.getId());
				pen.setMgTo(calcStore.getPeriod());
			} else {
				// не равны, создать запись нового периода
				isCreate = true;
			}

		}

		if (isCreate) {
			// создать запись нового периода
			Pen pen = Pen.builder()
					.withUsl(em.find(Usl.class, t.getUslOrg().getUslId()))
					.withOrg(em.find(Org.class, t.getUslOrg().getOrgId()))
					.withPenIn(t.getPenyaIn())
					.withPenOut(penyaOut)
					.withPenChrg(penChrgRound)
					.withPenCorr(t.getPenyaCorr())
					.withPenPay(t.getPenyaPay())
					.withDays(t.getDays())
					.withKart(kart)
					.withMgFrom(calcStore.getPeriod())
					.withMgTo(calcStore.getPeriod())
					.withMg(t.getMg())
					.build();
			em.persist(pen);
		}
	}

	/**
	 * перенаправить пеню на услугу и организацию по справочнику REDIR_PAY
	 * @param kart - лицевой счет
	 * @param lst - входящая коллекция долгов и пени
	 */
	private void redirectPen(Kart kart, List<SumPenRec> lst) {
		// произвести перенаправление начисления пени, по справочнику
		ArrayList<SumPenRec> lstAdd = new ArrayList<SumPenRec>();
		for (SumPenRec t : lst) {
			String uslId = t.getUslOrg().getUslId();
			Integer orgId = t.getUslOrg().getOrgId();
			UslOrg uo = refMng.getUslOrgRedirect(t.getUslOrg(), kart, 0);
			if (!uo.getUslId().equals(uslId)
					|| !uo.getOrgId().equals(orgId)) {
				// выполнить переброску, если услуга или организация - другие
				SumPenRec rec = lst.stream().filter(d->
						   d.getUslOrg().getUslId().equals(uo.getUslId())
						&& d.getUslOrg().getOrgId().equals(uo.getOrgId()))
						.filter(d -> d.getMg().equals(t.getMg())) // тот же период
						.findFirst().orElse(null);
				if (rec == null) {
					// строка с долгом и пенёй не найдена по данному периоду, создать
					SumPenRec sumPenRec = SumPenRec.builder()
							.withChng(BigDecimal.ZERO)
							.withChrg(BigDecimal.ZERO)
							.withDebIn(BigDecimal.ZERO)
							.withDebOut(BigDecimal.ZERO)
							.withDebPay(BigDecimal.ZERO)
							.withDebRolled(BigDecimal.ZERO)
							.withPayCorr(BigDecimal.ZERO)
							.withPenyaChrg(t.getPenyaChrg())
							.withPenyaCorr(BigDecimal.ZERO)
							.withPenyaIn(BigDecimal.ZERO)
							.withPenyaPay(BigDecimal.ZERO)
							.withDays(0)
							.withMg(t.getMg())
							.withUslOrg(uo)
							.build();
					lstAdd.add(sumPenRec);
				} else {
					// строка найдена
					rec.setPenyaChrg(rec.getPenyaChrg().add(t.getPenyaChrg()));
				}
				log.info("Перенаправление пени сумма={}", t.getPenyaChrg());
				t.setPenyaChrg(BigDecimal.ZERO);
			}
		}
		// добавить новые записи перенаправленых сумм (если они будут)
		lst.addAll(lstAdd);
	}


}