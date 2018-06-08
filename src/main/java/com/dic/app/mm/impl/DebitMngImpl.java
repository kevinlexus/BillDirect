package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DebitMng;
import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ReferenceMng;
import com.dic.app.mm.ThreadMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.dao.CorrectPayDAO;
import com.dic.bill.dao.DebDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.KwtpDayDAO;
import com.dic.bill.dao.PenDAO;
import com.dic.bill.dao.PenDtDAO;
import com.dic.bill.dao.PenRefDAO;
import com.dic.bill.dao.PenUslCorrDAO;
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
	private ConfigApp config;
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
	private PenDtDAO penDtDao;
	@Autowired
	private PenRefDAO penRefDao;
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
	@CacheEvict(value = {"ReferenceMng.getUslOrgRedirect"}, allEntries = true)
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor=Exception.class)
	public void genDebitAll(String lsk, Date genDt, Integer debugLvl, RequestConfig reqConf) throws ErrorWhileChrgPen {
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
		log.info("Начало получения справочников");
		// справочник дат начала пени
		calcStore.setLstPenDt(penDtDao.findAll());// .setLstSprPenUsl(sprPenUslDao.findAll());
		log.info("Загружен справочник дат начала обязательства по оплате");
		// справочник ставок рефинансирования
		calcStore.setLstPenRef(penRefDao.findAll()); //.setLstStavrUsl(stavrUslDao.findAll());
		log.info("Загружен справочник ставок рефинансирования");
		//calcStore.getLstSprPenUsl().size();
		//log.info("Cправочник дат начала обязательства по оплате");

		//calcStore.getLstStavrUsl().size();
		//log.info("Загружен справочник ставок рефинансирования");

		// получить список лицевых счетов
		List<String> lstItem;
		// флаг - заставлять ли многопоточный сервис проверять маркер остановки главного процесса
		boolean isCheckStop;
		// тип выполнения (0 - по одному лс, вывести отчет в C_DEBPEN_USL_TEMP,
		//				   1 - вывести исх сальдо)
		if (lsk == null) {
			// по всем лиц.счетам
			lstItem = kartDao.getAll()
					.stream().map(t->t.getLsk()).collect(Collectors.toList());
			isCheckStop = true;
 		} else {
 			// по одному лиц.счету
 			lstItem = new ArrayList<String>(0);
 			lstItem.add(kartDao.getByLsk(lsk).getLsk());
			isCheckStop = false;
 		}

		// будет выполнено позже, в создании потока
		PrepThread<String> reverse = (item) -> {
			// сервис расчета задолженности и пени
			DebitMng debitMng = ctx.getBean(DebitMng.class);
			return debitMng.genDebit(item, calcStore, reqConf);
		};

		// вызвать в потоках
		try {
			threadMng.invokeThreads(reverse, calcStore, 15, lstItem, isCheckStop);
		} catch (InterruptedException | ExecutionException e) {
			log.error(Utl.getStackTraceString(e));
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
	 * @param sessionId - Id сессии
	 */
	@Async
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW, rollbackFor=Exception.class)
	public Future<CommonResult> genDebit(String lsk, CalcStore calcStore, RequestConfig reqConf) {
		long startTime = System.currentTimeMillis();
		log.info("НАЧАЛО расчета задолженности по лиц.счету {}", lsk);
		try {
			// заблокировать лиц.счет для расчета
			if (!config.aquireLock(reqConf.getRqn(), lsk)) {
				throw new RuntimeException("ОШИБКА БЛОКИРОВКИ лc.="+lsk);
			}
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
			localStore.setLstPayCorrFlow(correctPayDao.getCorrectPayByLsk(lsk, String.valueOf(period)));
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
						log.error(Utl.getStackTraceString(e));
						throw new RuntimeException("ОШИБКА в процессе начисления пени по лc.="+lsk);
					}
				})
				.collect(Collectors.toList());

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

			// получить задолженность, по которой рассчитывается пеня, по всем услугам
			BigDecimal sumDeb = lst.stream().map(t->t.getDebRolled()).reduce(BigDecimal.ZERO, BigDecimal::add);
			log.info("Задолженность для расчета пени:{}", sumDeb);

			for (SumPenRec t : lst) {
					// занулить текущую пеню по всем услугам, если общее исх.долг < 0
					if (sumDeb.compareTo(BigDecimal.ZERO) <= 0) {
					//	t.setPenyaChrg(BigDecimal.ZERO);
					}
					// рассчитать исходящее сальдо по пене, сохранить расчет
					save(calcStore, kart, localStore, t);
			}


			long endTime = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			log.info("ОКОНЧАНИЕ расчета задолженности по лиц.счету {}! Время расчета={} мс", lsk, totalTime);
	 } finally {
		 // разблокировать лицевой счет
		 config.getLock().unlockLsk(reqConf.getRqn(), lsk);
	 }
	CommonResult res = new CommonResult(lsk, 1111111111); // TODO 111111
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
		// округлить начисленную пеню
		BigDecimal penChrgRound = t.getPenyaChrg().setScale(2, RoundingMode.HALF_UP);
		// исх.сальдо по пене
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
		//log.info("usl={}, org={} debIn={}", t.getUslOrg().getUslId(),
		//		t.getUslOrg().getOrgId(), t.getPenyaIn());
		if (isCreate) {
			// создать запись нового периода
			if (t.getDebIn().compareTo(BigDecimal.ZERO) != 0
						|| t.getDebOut().compareTo(BigDecimal.ZERO) != 0
						|| t.getDebRolled().compareTo(BigDecimal.ZERO) != 0
						|| t.getChrg().compareTo(BigDecimal.ZERO) != 0
						|| t.getChng().compareTo(BigDecimal.ZERO) != 0
						|| t.getDebPay().compareTo(BigDecimal.ZERO) != 0
						|| t.getPayCorr().compareTo(BigDecimal.ZERO) != 0
					) {
				// если хотя бы одно поле != 0
				Usl usl = em.find(Usl.class, t.getUslOrg().getUslId());
				if (usl == null) {
					// так как внутри потока, то только RuntimeException
					throw new RuntimeException("Ошибка при сохранении записей долгов,"
							+ " некорректные данные в таблице SCOTT.DEB!"
							+ " Не найдена услуга с кодом usl="+t.getUslOrg().getUslId());
				}
				Org org = em.find(Org.class, t.getUslOrg().getOrgId());
				if (org == null) {
					// так как внутри потока, то только RuntimeException
					throw new RuntimeException("Ошибка при сохранении записей долгов,"
							+ " некорректные данные в таблице SCOTT.DEB!"
							+ " Не найдена организация с кодом org="+t.getUslOrg().getOrgId());
				}
				Deb deb = Deb.builder()
						.withUsl(usl)
						.withOrg(org)
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
			if (t.getPenyaIn().compareTo(BigDecimal.ZERO) != 0
					|| penyaOut.compareTo(BigDecimal.ZERO) != 0
					|| penChrgRound.compareTo(BigDecimal.ZERO) != 0
					|| t.getPenyaCorr().compareTo(BigDecimal.ZERO) != 0
					|| t.getPenyaPay().compareTo(BigDecimal.ZERO) != 0
					//|| !t.getDays().equals(0)
					|| t.getPenyaCorr().compareTo(BigDecimal.ZERO) != 0
				) {
				// если хотя бы одно поле != 0
/*				log.info("ПРОВЕРКА uslId={}, orgId={}, период={}",
						t.getUslOrg().getUslId(), t.getUslOrg().getOrgId(), t.getMg());
*/				//log.info("CREATE usl={}, org={}", t.getUslOrg().getUslId(),
				//		t.getUslOrg().getOrgId());
				Usl usl = em.find(Usl.class, t.getUslOrg().getUslId());
				if (usl == null) {
					// так как внутри потока, то только RuntimeException
					throw new RuntimeException("Ошибка при сохранении записей пени,"
							+ " некорректные данные в таблице SCOTT.PEN!"
							+ " Не найдена услуга с кодом usl="+t.getUslOrg().getUslId());
				}
				Org org = em.find(Org.class, t.getUslOrg().getOrgId());
				if (org == null) {
					// так как внутри потока, то только RuntimeException
					throw new RuntimeException("Ошибка при сохранении записей пени,"
							+ " некорректные данные в таблице SCOTT.PEN!"
							+ " Не найдена организация с кодом org="+t.getUslOrg().getOrgId());
				}

				Pen pen = Pen.builder()
						.withUsl(usl)
						.withOrg(org)
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
	}

	/**
	 * перенаправить пеню на услугу и организацию по справочнику REDIR_PAY
	 * @param kart - лицевой счет
	 * @param lst - входящая коллекция долгов и пени
	 * @return
	 */
	private void redirectPen(Kart kart, List<SumPenRec> lst) {
		// произвести перенаправление начисления пени, по справочнику
		//ArrayList<SumPenRec> lstOut = new ArrayList<SumPenRec>();

		ListIterator<SumPenRec> itr = lst.listIterator();
		while (itr.hasNext()) {
			SumPenRec t = itr.next();
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
				//log.info("ПОИСК mg={}, usl={}, org={}", t.getMg(), uo.getUslId(), uo.getOrgId());
				if (rec == null) {
					//log.info("ПЕРИОД mg={}, usl={}, org={} Не найден! size={}", t.getMg(), uo.getUslId(), uo.getOrgId(), lst.size());
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
					itr.add(sumPenRec);
				} else {
					// строка найдена
					//log.info("mg={}, usl={}, org={} найден", t.getMg(), uo.getUslId(), uo.getOrgId());
					rec.setPenyaChrg(rec.getPenyaChrg().add(t.getPenyaChrg()));
				}
				//log.info("Перенаправление пени сумма={}", t.getPenyaChrg());
				t.setPenyaChrg(BigDecimal.ZERO);
			}
		}
	}


}