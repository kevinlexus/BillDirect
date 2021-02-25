package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.NaborDAO;
import com.dic.bill.dao.UslDAO;
import com.dic.bill.dto.*;
import com.dic.bill.mm.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис расчета начисления
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
public class GenChrgProcessMngImpl implements GenChrgProcessMng {

    private final NaborMng naborMng;
    private final KartMng kartMng;
    private final KartPrMng kartPrMng;
    private final MeterMng meterMng;
    private final MeterDAO meterDao;
    private final NaborDAO naborDAO;
    private final UslDAO uslDao;
    private final ConfigApp config;
    private final SprParamMng sprParamMng;

    @PersistenceContext
    private EntityManager em;

    public GenChrgProcessMngImpl(NaborMng naborMng, KartMng kartMng,
                                 KartPrMng kartPrMng, MeterMng meterMng, MeterDAO meterDao, NaborDAO naborDAO, UslDAO uslDao,
                                 ConfigApp config, SprParamMng sprParamMng) {
        this.naborMng = naborMng;
        this.kartMng = kartMng;
        this.kartPrMng = kartPrMng;
        this.meterMng = meterMng;
        this.meterDao = meterDao;
        this.naborDAO = naborDAO;
        this.uslDao = uslDao;
        this.config = config;
        this.sprParamMng = sprParamMng;
    }

    /**
     * Рассчитать начисление
     * Внимание! Расчет идёт по помещению (помещению), но информация группируется по лиц.счету(Kart)
     * так как теоретически может быть одинаковая услуга на разных лиц.счетах, но на одной помещению!
     * ОПИСАНИЕ: https://docs.google.com/document/d/1mtK2KdMX4rGiF2cUeQFVD4HBcZ_F0Z8ucp1VNK8epx0/edit
     *
     * @param reqConf - конфиг запроса
     * @param klskId  - klskId помещения
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRES_NEW,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class) //
    public void genChrg(RequestConfigDirect reqConf, long klskId) throws ErrorWhileChrg {
        // заблокировать объект Ko для расчета
        if (!config.getLock().aquireLockId(reqConf.getRqn(), 1, klskId, 60)) {
            throw new ErrorWhileChrg("ОШИБКА БЛОКИРОВКИ klskId=" + klskId);
        }
        try {
            CalcStore calcStore = reqConf.getCalcStore();
            Ko ko = em.getReference(Ko.class, klskId);

            // создать локальное хранилище объемов
            ChrgCountAmountLocal chrgCountAmountLocal = new ChrgCountAmountLocal();
            // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
            int parVarCntKpr =
                    Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"), 0D).intValue();
            // параметр учета проживающих для капремонта
            int parCapCalcKprTp =
                    Utl.nvl(sprParamMng.getN1("CAP_CALC_KPR_TP"), 0D).intValue();

            //ChrgCount chrgCount = new ChrgCount();
            // выбранные услуги для формирования
            List<Usl> lstSelUsl = new ArrayList<>();
            if (Utl.in(reqConf.getTp(), 0, 4) && reqConf.getUsl() != null) {
                // начисление по выбранной услуге, начисление по одной услуге, для автоначисления
                lstSelUsl.add(reqConf.getUsl());
            } else if (Utl.in(reqConf.getTp(), 3)) {
                // начисление для распределения по вводу
                // добавить услуги для ограничения формирования только по ним
                lstSelUsl.add(reqConf.getVvod().getUsl());
            }

            // все действующие счетчики объекта и их объемы
            List<SumMeterVol> lstMeterVol = meterDao.findMeterVolUsingKlsk(ko.getId(),
                    calcStore.getCurDt1(), calcStore.getCurDt2());
            /*System.out.println("Счетчики:");
            for (SumMeterVol t : lstMeterVol) {
                log.trace("t.getMeterId={}, t.getUslId={}, t.getDtTo={}, t.getDtFrom={}, t.getVol={}",
                        t.getMeterId(), t.getUslId(), t.getDtTo(), t.getDtFrom(), t.getVol());
            }*/
            // получить объемы по счетчикам в пропорции на 1 день их работы
            List<UslMeterDateVol> lstDayMeterVol = meterMng.getPartDayMeterVol(lstMeterVol,
                    calcStore);

            Calendar c = Calendar.getInstance();

            // получить действующие, отсортированные услуги по помещению (по всем счетам)
            // перенести данный метод внутрь genVolPart, после того как будет реализованна архитектурная возможность
            // отключать услугу в течении месяца
            List<Nabor> lstNabor = naborMng.getActualNabor(ko, null);

            // очистить информационные строки по льготам
            lstNabor.stream().map(Nabor::getKart).distinct().forEach(t ->
                    t.getChargePrep().removeIf(chargePrep -> chargePrep.getTp().equals(9)));

            // РАСЧЕТ по блокам:

            // 1. Основные услуги
            // цикл по дням месяца
            int part = 1;
            log.trace("Расчет объемов услуг, до учёта экономии ОДН");
            for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                    .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
                genVolPart(chrgCountAmountLocal, reqConf, parVarCntKpr,
                        parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part, lstNabor);
            }

            // кроме распределения объемов (там нечего еще считать, нет экономии ОДН
            if (!Utl.in(reqConf.getTp(), 3, 4)) {
                // 2. распределить экономию ОДН по услуге, пропорционально объемам
                log.trace("Распределение экономии ОДН");
                distODNeconomy(chrgCountAmountLocal, ko, lstSelUsl);

                // 3. Зависимые услуги, которые необходимо рассчитать после учета экономии ОДН в основных расчетах
                // цикл по дням месяца (например calcTp=47 - Тепл.энергия для нагрева ХВС или calcTp=19 - Водоотведение)
                part = 2;
                log.trace("Расчет объемов услуг, после учёта экономии ОДН");
                for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                        .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
                    genVolPart(chrgCountAmountLocal, reqConf, parVarCntKpr,
                            parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part, lstNabor);
                }
            }

            // 4. Округлить объемы
            chrgCountAmountLocal.roundVol();
            //chrgCountAmountLocal.printVolAmntChrg();

            if (reqConf.getTp() == 3) {
                // 5. Добавить в объемы по вводу
                reqConf.getChrgCountAmount().append(chrgCountAmountLocal);
            }

            chrgCountAmountLocal.printVolAmnt(null, "После округления");

            if (!Utl.in(reqConf.getTp(), 3, 4)) {
                // 6. Сгруппировать строки начислений для записи в C_CHARGE
                //chrgCountAmountLocal.printVolAmntChrg();
                chrgCountAmountLocal.groupUslVolChrg();

                // 7. Умножить объем на цену (расчет в рублях), сохранить в C_CHARGE, округлить для ГИС ЖКХ
                chrgCountAmountLocal.saveChargeAndRound(ko);

                // 9. Сохранить фактическое наличие счетчика, в случае отсутствия объема, для формирования статистики
                chrgCountAmountLocal.saveFactMeterTp(ko, lstMeterVol, reqConf.getCalcStore().getCurDt2());
            }

            if (reqConf.getTp() == 4) {
                // 10. по операции - начисление по одной услуге, для автоначисления - по нормативу
                // заполнить итоговый объем
                reqConf.getChrgCountAmount().setResultVol(chrgCountAmountLocal.getAmntVolByUsl(reqConf.getUsl()));
            }

        } catch (WrongParam wrongParam) {
            log.error(Utl.getStackTraceString(wrongParam));
            throw new ErrorWhileChrg("Ошибка в использовании параметра");
        } finally {
            // разблокировать помещение
            config.getLock().unlockId(reqConf.getRqn(), 1, klskId);
            //log.info("******* klskId={} разблокирован после расчета", klskId);
        }
    }

    /**
     * Расчет объема по услугам
     *
     * @param chrgCountAmountLocal - локальное хранилище объемов, по помещению
     * @param reqConf              - запрос
     * @param parVarCntKpr         - параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
     * @param parCapCalcKprTp      - параметр учета проживающих для капремонта
     * @param ko                   - объект Ko помещения
     * @param lstMeterVol          - объемы по счетчикам
     * @param lstSelUsl            - список услуг для расчета
     * @param lstDayMeterVol       - хранилище объемов по счетчикам
     * @param curDt                - дата расчета
     * @param part                 - группа расчета (услуги рассчитываемые до(1) /после(2) рассчета ОДН
     * @param lstNabor             - список действующих услуг
     * @throws ErrorWhileChrg - ошибка во время расчета
     * @throws WrongParam     - ошибочный параметр
     */
    private void genVolPart(ChrgCountAmountLocal chrgCountAmountLocal,
                            RequestConfigDirect reqConf, int parVarCntKpr,
                            int parCapCalcKprTp, Ko ko, List<SumMeterVol> lstMeterVol, List<Usl> lstSelUsl,
                            List<UslMeterDateVol> lstDayMeterVol, Date curDt, int part, List<Nabor> lstNabor)
            throws ErrorWhileChrg, WrongParam {

        CalcStore calcStore = reqConf.getCalcStore();
        //boolean isExistsMeterColdWater = false;
        //boolean isExistsMeterHotWater = false;
        //BigDecimal volColdWater = BigDecimal.ZERO;
        //BigDecimal volHotWater = BigDecimal.ZERO;

        // объем по услуге, за рассчитанный день

        Map<String, UslPriceVolKart> mapUslPriceVol = new HashMap<>(30);

        for (Nabor nabor : lstNabor) {
            // получить основной лиц счет по связи klsk помещения
            Kart kartMainByKlsk = em.getReference(Kart.class, kartMng.getKartMainLsk(nabor.getKart()));

/*
            if (reqConf.getTp()==0 && nabor.getKart().getLsk().equals("15042021")) {
                log.error("CHECKERR!");
                throw new RuntimeException("CHECKERR!");
            } ss
*/
            //log.trace("Основной лиц.счет lsk={}", kartMainByKlsk.getLsk());
            if (nabor.getUsl().isMain() && (lstSelUsl.size() == 0 || lstSelUsl.contains(nabor.getUsl()))
                    && (part == 1 && !Utl.in(nabor.getUsl().getFkCalcTp(), 47, 19) ||
                    part == 2 && Utl.in(nabor.getUsl().getFkCalcTp(), 47, 19)) // фильтр очередности расчета
            ) {
                // РАСЧЕТ по основным услугам (из набора услуг или по заданным во вводе)
/*
                log.trace("part={}, {}: lsk={}, uslId={}, fkCalcTp={}, dt={}",
                        part,
                        reqConf.getTpName(),
                        nabor.getKart().getLsk(), nabor.getUsl().getId(),
                        nabor.getUsl().getFkCalcTp(), Utl.getStrFromDate(curDt));
*/
                final Integer fkCalcTp = nabor.getUsl().getFkCalcTp();
                final BigDecimal naborNorm = Utl.nvl(nabor.getNorm(), BigDecimal.ZERO);
                final BigDecimal naborVol = Utl.nvl(nabor.getVol(), BigDecimal.ZERO);
                final BigDecimal naborVolAdd = Utl.nvl(nabor.getVolAdd(), BigDecimal.ZERO);
                // услуга с которой получить объем (иногда выполняется перенаправление, например для fkCalcTp=31)
                final Usl factUslVol = nabor.getUsl().getMeterUslVol() != null ?
                        nabor.getUsl().getMeterUslVol() : nabor.getUsl();
                // ввод
                final Vvod vvod = nabor.getVvod();
                final boolean isForChrg = nabor.isValid(false);
                // признаки 0 зарег. и наличия счетчика от связанной услуги
                Boolean isLinkedEmpty = null;
                Boolean isLinkedExistMeter = null;

                // тип распределения ввода
                Integer distTp = 0;
                BigDecimal vvodVol = BigDecimal.ZERO;
                boolean isChargeInNotHeatingPeriod = false;
                if (vvod != null) {
                    distTp = vvod.getDistTp();
                    vvodVol = Utl.nvl(vvod.getKub(), BigDecimal.ZERO);
                    isChargeInNotHeatingPeriod = vvod.getIsChargeInNotHeatingPeriod() != null
                            && vvod.getIsChargeInNotHeatingPeriod();
                }
                Kart kartMain;
                if (Utl.in(nabor.getKart().getTp().getCd(), "LSK_TP_ADDIT", "LSK_TP_RSO")) {
                    // дополнит.счета Капрем., РСО
                    // получить родительский лиц.счет, если указан явно
                    kartMain = kartMainByKlsk;
                    // ред. 18.04.19 по просьбе КИС: Алена. Вылезло еще кое что. Посмотрите? 82005413 по услуге 015
                    // стоят кубы по привязки с л.сч.86019516 16,85(на 5 человек), а должны считаться
                    // как на 1 собств. в этой карточке 3,37.
/*
                    if (nabor.getKart().getParentKart() != null) {
                        kartMain = nabor.getKart().getParentKart();
                        //kartMain = kartMainByKlsk;
                    } else {
                        kartMain = kartMainByKlsk;
                    }
*/
                } else {
                    // основные лиц.счета - взять текущий лиц.счет
                    kartMain = nabor.getKart();
                }
                // получить цены по услуге по лицевому счету из набора услуг!
                final DetailUslPrice detailUslPrice = naborMng.getDetailUslPrice(kartMain, nabor);

/*
                for (Kart kart : ko.getKart()) {
                    for (KartPr kartPr : kart.getKartPr()) {
                        kartPr.getStatePr().size();
                        for (StatePr statePr : kartPr.getStatePr()) {
                            log.info(statePr.getStatusPr().getTp().getCd());
                        }
                    }
                }
*/
                // наличие счетчика
                boolean isMeterExist = false;
                if (Utl.in(fkCalcTp, 17, 18, 31, 52, 53, 54, 55, 56)) {
                    // х.в.,г.в узнать, работал ли хоть один счетчик в данном дне
                    isMeterExist = meterMng.isExistAnyMeter(lstMeterVol, factUslVol.getId(), curDt);
                }
                CountPers countPers = getCountPersAmount(parVarCntKpr, parCapCalcKprTp, curDt,
                        nabor, kartMain, isMeterExist);

                SocStandart socStandart = null;
                // наличие счетчика х.в.
                boolean isColdMeterExist = false;
                // наличие счетчика г.в.
                boolean isHotMeterExist = false;
                BigDecimal tempVol = BigDecimal.ZERO;
                // объемы
                BigDecimal dayVol = BigDecimal.ZERO;
                // объем по х.в. для водоотведения
                BigDecimal dayColdWaterVol = BigDecimal.ZERO;
                BigDecimal dayColdWaterVolOverSoc = BigDecimal.ZERO;
                // объем по г.в. для водоотведения
                BigDecimal dayHotWaterVol = BigDecimal.ZERO;
                BigDecimal dayHotWaterVolOverSoc = BigDecimal.ZERO;
                BigDecimal dayVolOverSoc = BigDecimal.ZERO;

                // площади (взять с текущего лиц.счета)
                final BigDecimal kartArea = Utl.nvl(nabor.getKart().getOpl(), BigDecimal.ZERO);
                BigDecimal areaOverSoc = BigDecimal.ZERO;
                int vvodDistTp = naborMng.getVvodDistTp(lstNabor, nabor.getUsl().getParentUsl());
                boolean isMunicipal = nabor.getKart().getStatus().getId().equals(1);

                // расчет
                if (Utl.in(fkCalcTp, 25) // Текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                        || fkCalcTp.equals(7) && isMunicipal // Найм (только по муниципальным помещениям) расчет на м2
                        || (fkCalcTp.equals(24) || fkCalcTp.equals(32) // Прочие услуги, расчитываемые как расценка * норматив * общ.площадь
                        && !isMunicipal)// или 32 услуга, только не по муниципальному фонду
                        || fkCalcTp.equals(36)// Вывоз жидких нечистот и т.п. услуги
                        || fkCalcTp.equals(37) && !isMunicipal // Капремонт в немуницип.фонде
                        || fkCalcTp.equals(50) && vvodDistTp == 4 // если нет счетчика ОДПУ
                        || fkCalcTp.equals(51) && (vvodDistTp == 1 || vvodDistTp == 8) // проп.площади или для информации
                ) {
                    if (fkCalcTp.equals(25)) {
                        // текущее содержание - получить соц.норму
                        socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                        dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                    } else if (fkCalcTp.equals(37)) {
                        // капремонт
                        if (countPers.capPriv != null) {
                            // есть льгота, добавить информацию, если уже не была добавлена в этом периоде
                            if (!chrgCountAmountLocal.isCapPrivAdded()) {
                                ChargePrep chargePrep = new ChargePrep();
                                nabor.getKart().getChargePrep().add(chargePrep);
                                chargePrep.setKart(nabor.getKart());
                                chargePrep.setUsl(nabor.getUsl());
                                chargePrep.setTp(9);
                                chargePrep.setSpk(countPers.capPriv);
                                chrgCountAmountLocal.setCapPrivAdded(true);
                            }
                        } else {
                            dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                        }
                    } else {
                        // прочие услуги
                        dayVol = kartArea.multiply(calcStore.getPartDayMonth());
                    }
                } else if (Utl.in(fkCalcTp, 17, 18, 31, 52)) {
                    // Х.В., Г.В., без уровня соцнормы/свыше, электроэнергия
                    // получить объем по нормативу в доле на 1 день
                    if (Utl.in(fkCalcTp, 17, 31) || (Utl.in(fkCalcTp, 18, 52) &&
                            (!Utl.nvl(kartMain.getIsKran1(), false) ||
                                    isMeterExist || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT1"),// кран из системы отопления (не счетчик) -
                                    sprParamMng.getD1("MONTH_HEAT2")) // начислять только в отопительный период
                            ))) {
                        if (reqConf.getTp() == 4) {
                            // начисление по выбранной услуге, по нормативу, для автоначисления
                            isMeterExist = false;
                        }

                        // получить соцнорму
                        socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                        if (isMeterExist) {
                            // получить объем по счетчику в пропорции на 1 день его работы
                            UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                    .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                    .findFirst().orElse(null);
                            if (partVolMeter != null) {
                                tempVol = partVolMeter.vol;
                            }
                        } else {
                            // норматив в пропорции на 1 день месяца
                            tempVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                        }

                        dayVol = tempVol;
                    }
                } else if (Utl.in(fkCalcTp, 53)) {
                    // Компонент на тепл энерг. для Г.В.
                    // получить объем по нормативу в доле на 1 день
                    // получить соцнорму
                    Optional<Usl> uslParentOpt = uslDao.findByCd("COMPHW");
                    if (uslParentOpt.isPresent()) {
                        // получить норматив, кол-во прож, объемы счетчика, от родительской услуги "COMPHW"
                        Optional<Nabor> naborParentOpt = naborDAO.findByKartAndUsl(nabor.getKart(),
                                uslParentOpt.get());
                        if (naborParentOpt.isPresent()) {
                            CountPers countPersParent = getCountPersAmount(parVarCntKpr, parCapCalcKprTp, curDt,
                                    naborParentOpt.get(), kartMain, isMeterExist);
                            socStandart = kartPrMng.getSocStdtVol(kartArea, naborParentOpt.get(), countPersParent);
                            if (isMeterExist) {
                                // получить объем по счетчику в пропорции на 1 день его работы
                                UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                        .filter(t -> t.usl.equals(naborParentOpt.get().
                                                getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                        .findFirst().orElse(null);
                                if (partVolMeter != null) {
                                    tempVol = partVolMeter.vol;
                                }
                            } else {
                                // норматив в пропорции на 1 день месяца
                                tempVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                            }
                            // умножить на норматив гКал * объем м3
                            dayVol = tempVol.multiply(nabor.getNorm());

                        } else {
                            log.error("Для дочерней услуги USL.USL={}, не найдена запись набора с NABOR.LSK={} и NABOR.USL={}",
                                    nabor.getUsl().getId(), nabor.getKart().getLsk(),
                                    uslParentOpt.get().getId());
                        }
                    } else {
                        log.error("Для дочерней услуги USL.USL={}, не найдена услуга по USL.CD=\"COMPHW\"",
                                nabor.getUsl().getId());
                    }
                } else if (Utl.in(fkCalcTp, 19)) {
                    // Водоотведение без уровня соцнормы/свыше
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне

                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);

                    List<UslPriceVolKart> lstColdHotWater =
                            chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                    .filter(t -> t.getDt().equals(curDt)
                                                    && t.getKart().getKoKw().equals(nabor.getKart().getKoKw())
                                                    && t.getUsl().getIsUseVolCan()
                                            //&& Utl.in(t.getUsl().getFkCalcTp(), 17, 18) убрал. ред.02.02.21
                                    ).collect(Collectors.toList());
                    // сложить предварительно рассчитанные объемы х.в.+г.в., найти признаки наличия счетчиков
                    for (UslPriceVolKart t : lstColdHotWater) {
                        if (t.getUsl().getFkCalcTp().equals(17)) {
                            // х.в.
                            dayColdWaterVol = dayColdWaterVol.add(t.getVol());
                            isColdMeterExist = t.isMeter();
                        } else {
                            // г.в.
                            dayHotWaterVol = dayHotWaterVol.add(t.getVol());
                            isHotMeterExist = t.isMeter();
                        }
                    }
                } else if (Utl.in(fkCalcTp, 14)) {
                    // Отопление гкал. без уровня соцнормы/свыше
                    if (Utl.nvl(kartMain.getPot(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) != 0) {
                        // есть показания по Индивидуальному счетчику отопления
                        tempVol = Utl.nvl(kartMain.getMot(), BigDecimal.ZERO);
                    } else {
                        if (kartArea.compareTo(BigDecimal.ZERO) != 0) {
                            if (distTp.equals(1)) {
                                // есть ОДПУ по отоплению гкал, начислить по распределению
                                tempVol = naborVol;
                            } else if (Utl.in(distTp, 4, 5)) {
                                // нет ОДПУ по отоплению гкал, начислить по нормативу с учётом отопительного сезона
                                if (isChargeInNotHeatingPeriod) {
                                    // начислять и в НЕотопительном периоде
                                    tempVol = kartArea.multiply(naborNorm);
                                } else {
                                    // начислять только в отопительном периоде
                                    if (Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT3"),
                                            sprParamMng.getD1("MONTH_HEAT4"))) {
                                        tempVol = kartArea.multiply(naborNorm);
                                    }
                                }
                            }
                        }

                    }
                    //  в доле на 1 день
                    // помещение с проживающими
                    dayVol = tempVol.multiply(calcStore.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 54)) {
                    // Отопление гкал. с уровнем соцнормы/свыше (Полыс.)
                    socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                    if (isMeterExist) {
                        // получить объем по счетчику в пропорции на 1 день его работы
                        UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                .findFirst().orElse(null);
                        if (partVolMeter != null) {
                            tempVol = partVolMeter.vol.multiply(calcStore.getPartDayMonth());
                        }
                        //  в доле на 1 день - по счетчику - весь объем по льготной цене
                        dayVol = tempVol;
                    } else {
                        if (kartArea.compareTo(BigDecimal.ZERO) != 0) {
                            if (distTp.equals(1)) {
                                // есть ОДПУ по отоплению гкал, начислить по распределению
                                tempVol = naborVol.multiply(calcStore.getPartDayMonth());
                            } else if (Utl.in(distTp, 4, 5)) {
                                // нет ОДПУ по отоплению гкал, начислить по нормативу с учётом отопительного сезона
                                if (isChargeInNotHeatingPeriod || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT3"),
                                        sprParamMng.getD1("MONTH_HEAT4"))) {
                                    tempVol = kartArea.multiply(naborNorm).multiply(calcStore.getPartDayMonth());
                                }
                            }
                        }
                        //  в доле на 1 день
                        dayVol = tempVol.multiply(socStandart.procNorm);
                        dayVolOverSoc = tempVol.subtract(dayVol);
                    }

                } else if (Utl.in(fkCalcTp, 55, 56)) {
                    // Х.В., Г.В., с соцнормой/свыше
                    // получить объем по нормативу в доле на 1 день
                    if (Utl.in(fkCalcTp, 55) || (Utl.in(fkCalcTp, 56) &&
                            (!Utl.nvl(kartMain.getIsKran1(), false) ||
                                    isMeterExist || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT1"),// кран из системы отопления (не счетчик) -
                                    sprParamMng.getD1("MONTH_HEAT2")) // начислять только в отопительный период
                            ))) {
                        if (reqConf.getTp() == 4) {
                            // начисление по выбранной услуге, по нормативу, для автоначисления
                            isMeterExist = false;
                        }

                        if (isMeterExist) {
                            // получить объем по счетчику в пропорции на 1 день его работы
                            UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                    .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                    .findFirst().orElse(null);
                            if (partVolMeter != null) {
                                tempVol = partVolMeter.vol;
                            }
                            // получить соцнорму
                            socStandart = kartPrMng.getSocStdtVol(
                                    tempVol.multiply(BigDecimal.valueOf(Utl.getCntDaysByDate(curDt))),
                                    nabor, countPers);

                            dayVol = tempVol.multiply(socStandart.procNorm);
                            dayVolOverSoc = tempVol.subtract(dayVol);
                        } else {
                            // норматив в пропорции на 1 день месяца
                            // получить соцнорму
                            socStandart = kartPrMng.getSocStdtVol(BigDecimal.ZERO, nabor, countPers);
                            dayVol = socStandart.vol.multiply(calcStore.getPartDayMonth());
                        }
                    }
                } else if (Utl.in(fkCalcTp, 57)) {
                    // Водоотведение с соцнормой/свыше
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне

                    List<UslPriceVolKart> lstColdHotWater =
                            chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                    .filter(t -> t.getDt().equals(curDt)
                                            && t.getKart().getKoKw().equals(nabor.getKart().getKoKw())
                                            && t.getUsl().getIsUseVolCan()
                                    ).collect(Collectors.toList());
                    // сложить предварительно рассчитанные объемы х.в.+г.в., найти признаки наличия счетчиков
                    BigDecimal coldWaterVol = BigDecimal.ZERO;
                    BigDecimal hotWaterVol = BigDecimal.ZERO;
                    BigDecimal ttt = BigDecimal.ZERO;
                    for (UslPriceVolKart t : lstColdHotWater) {
                        if (t.getUsl().getFkCalcTp().equals(55)) {
                            // х.в.
                            coldWaterVol = coldWaterVol.add(t.getVol().add(t.getVolOverSoc()));
                            isColdMeterExist = t.isMeter();
                            ttt=ttt.add(t.getVol().add(t.getVolOverSoc()));
                            log.info("usl={}, vol={}", t.getUsl().getId(), t.getVol().add(t.getVolOverSoc()));
                        } else if (t.getUsl().getFkCalcTp().equals(56)) {
                            // г.в.
                            hotWaterVol = hotWaterVol.add(t.getVol().add(t.getVolOverSoc()));
                            isHotMeterExist = t.isMeter();
                            ttt=ttt.add(t.getVol().add(t.getVolOverSoc()));
                            log.info("usl={}, vol={}", t.getUsl().getId(), t.getVol().add(t.getVolOverSoc()));
                        }
                    }
                    log.info("ttt={}", ttt);

                    BigDecimal amountVol = coldWaterVol.add(hotWaterVol);
                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(
                            amountVol.multiply(BigDecimal.valueOf(Utl.getCntDaysByDate(curDt))), nabor, countPers);
                    BigDecimal partSoc = amountVol.multiply(socStandart.procNorm);
                    BigDecimal partOverSoc = amountVol.subtract(partSoc);
                    BigDecimal ratio = BigDecimal.ONE;
                    if (amountVol.compareTo(BigDecimal.ZERO) != 0) {
                        ratio = coldWaterVol.divide(amountVol, 7, RoundingMode.HALF_UP);
                    }
                    dayColdWaterVol = partSoc.multiply(ratio);
                    dayHotWaterVol = partSoc.subtract(dayColdWaterVol);

                    dayColdWaterVolOverSoc = partOverSoc.multiply(ratio);
                    dayHotWaterVolOverSoc = partOverSoc.subtract(dayColdWaterVolOverSoc);

                } else if (Utl.in(fkCalcTp, 12)) {
                    // Антенна, код.замок
                    dayVol = calcStore.getPartDayMonth();
                } else if (Utl.in(fkCalcTp, 20, 21, 23)) {
                    // Х.В., Г.В., Эл.Эн. содерж.общ.им.МКД, Эл.эн.гараж
                    if (Utl.in(fkCalcTp, 20, 21)) {
                        if (nabor.getUsl().getParentUsl() != null) {
                            // получить наличие счетчика из родительской услуги
                            UslPriceVolKart uslPriceVolKart = mapUslPriceVol.get(nabor.getUsl().getParentUsl().getId());
                            isMeterExist = uslPriceVolKart != null && uslPriceVolKart.isMeter();
                        } else {
                            log.error("Не найдена главная услуга PARENT_USL в справочнике услуг: usl={}",
                                    nabor.getUsl().getId());
                        }
                    }
                    dayVol = naborVolAdd.multiply(calcStore.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 34)) {
                    // Повыш.коэфф Полыс
                    if (nabor.getUsl().getParentUsl() != null) {
                        dayVol = calcStore.getPartDayMonth().multiply(naborNorm);
                    } else {
                        throw new ErrorWhileChrg("ОШИБКА! По услуге usl.id=" + nabor.getUsl().getId() +
                                " отсутствует PARENT_USL");
                    }
                } else if (Utl.in(fkCalcTp, 44)) {
                    // Повыш.коэфф Кис
                    // получить объем из родительской услуги
                    dayVol = getParentVol(mapUslPriceVol, naborNorm, "015");
                    dayVol = dayVol.add(getParentVol(mapUslPriceVol, naborNorm, "162"));
                } else if (fkCalcTp.equals(49)) {
                    // Вывоз мусора - кол-во прожив * цену (Кис.)
                    //area = kartArea;
                    dayVol = BigDecimal.valueOf(countPers.kprNorm).multiply(calcStore.getPartDayMonth());
                } else if (fkCalcTp.equals(47)) {
                    // Тепл.энергия для нагрева ХВС (Кис.)
                    //area = kartArea;
                    Optional<Usl> uslParentOpt = uslDao.findByCd("х.в. для гвс");
                    // Usl uslLinked = uslDao.getByCd("х.в. для гвс"); убрал 04.02.2021
                    if (uslParentOpt.isPresent()) {
                        Usl uslLinked = uslParentOpt.get();
                        BigDecimal vvodVol2 = ko.getKart().stream()
                                .flatMap(t -> t.getNabor().stream())
                                .filter(t -> t.getUsl().equals(uslLinked))
                                .map(t -> t.getVvod().getKub())
                                .findFirst().orElse(BigDecimal.ZERO);

                        // получить объем по расчетному дню связанной услуги
                        UslPriceVolKart uslPriceVolKart = chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                .filter(t -> t.getDt().equals(curDt)
                                        && t.getKart().getKoKw().equals(nabor.getKart().getKoKw())
                                        && t.getUsl().equals(uslLinked))
                                .findFirst().orElse(null);

                        if (uslPriceVolKart != null) {
                            isLinkedEmpty = uslPriceVolKart.isEmpty();
                            isLinkedExistMeter = uslPriceVolKart.isMeter();
                            if (vvodVol2.compareTo(BigDecimal.ZERO) != 0) {
                                dayVol = uslPriceVolKart.getVol().divide(vvodVol2, 20, BigDecimal.ROUND_HALF_UP)
                                        .multiply(vvodVol);
                            }
                        }
                    } else {
                        log.error("Для дочерней услуги USL.USL={}, не найдена услуга по USL.CD=\"х.в. для гвс\"",
                                nabor.getUsl().getId());
                    }
                } else if (fkCalcTp.equals(6) && countPers.kpr > 0) {
                    // Очистка выгр.ям (Полыс.) (при наличии проживающих)
                    // просто взять цену
                    dayVol = new BigDecimal(countPers.kprNorm).multiply(calcStore.getPartDayMonth());
                }

                UslPriceVolKart uslPriceVolKart;
                if (Utl.in(nabor.getUsl().getFkCalcTp(), 19, 57)) {
                    // водоотведение, добавить составляющие по х.в. и г.в.
                    // было ли учтено кол-во проживающих? для устранения удвоения в стате по водоотведению
                    // ред. 05.04.19 - Кис. попросили делать пустую строку, даже если нет объема, для статы
                    uslPriceVolKart = buildVol(curDt, calcStore, nabor, null, null,
                            kartMain, detailUslPrice, countPers, socStandart, isColdMeterExist,
                            dayColdWaterVol, dayColdWaterVolOverSoc, kartArea, areaOverSoc, isForChrg);
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                    // ред. 05.04.19 - Кис. попросили делать пустую строку, даже если нет объема, для статы
                    // уже были учтены проживающие
                    countPers.kpr = 0;
                    countPers.kprNorm = 0;
                    countPers.kprOt = 0;
                    countPers.kprWr = 0;
                    uslPriceVolKart = buildVol(curDt, calcStore, nabor, null, null,
                            kartMain, detailUslPrice, countPers, socStandart, isHotMeterExist,
                            dayHotWaterVol, dayHotWaterVolOverSoc, kartArea, areaOverSoc, isForChrg);
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                } else {
                    // прочие услуги
                    uslPriceVolKart = buildVol(curDt, calcStore, nabor, isLinkedEmpty, isLinkedExistMeter,
                            kartMain, detailUslPrice, countPers, socStandart, isMeterExist,
                            dayVol, dayVolOverSoc, kartArea, areaOverSoc, isForChrg);
                    if (Utl.in(nabor.getUsl().getFkCalcTp(), 17, 18, 31, 53)) {
                        // по х.в., г.в., эл.эн.
                        // сохранить расчитанный объем по расчетному дню, (используется для услуги Повыш коэфф.)
                        mapUslPriceVol.put(nabor.getUsl().getId(), uslPriceVolKart);
                    }
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                }


                //                        log.info("******* RESID={}", uslPriceVolKart.isResidental);

                    /*
                    if (Utl.in(uslPriceVolKart.usl.getId(),"003")) {
                        log.info("РАСЧЕТ ДНЯ:");
                        log.info("dt:{}-{} usl={} org={} cnt={} " +
                                        "empt={} stdt={} " +
                                        "prc={} prcOv={} prcEm={} " +
                                        "vol={} volOv={} volEm={} ar={} arOv={} " +
                                        "arEm={} Kpr={} Ot={} Wrz={}",
                                Utl.getStrFromDate(uslPriceVolKart.dtFrom, "dd"), Utl.getStrFromDate(uslPriceVolKart.dtTo, "dd"),
                                uslPriceVolKart.usl.getId(), uslPriceVolKart.org.getId(), uslPriceVolKart.isMeter, uslPriceVolKart.isEmpty,
                                uslPriceVolKart.socStdt, uslPriceVolKart.price, uslPriceVolKart.priceOverSoc, uslPriceVolKart.priceEmpty,
                                uslPriceVolKart.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.volOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.volEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.area.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.areaOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.areaEmpty.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.kpr.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.kprOt.setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.kprWr.setScale(4, BigDecimal.ROUND_HALF_UP));
                    }
*/

/*
                if (lstSelUsl.size() == 0 && nabor.getUsl().getId().equals("015") || nabor.getUsl().getId().equals("099")) {
                    log.info("************!!!!!!!! usl={}, vol={}, dt={}", nabor.getUsl().getId(), dayVol, Utl.getStrFromDate(curDt));
                }
                if (lstSelUsl.size() == 0 && nabor.getUsl().getId().equals("015") || nabor.getUsl().getId().equals("099")) {
                    for (UslVolKartGrp t : calcStore.getChrgCountAmount().getLstUslVolKartGrp()) {
                        if (t.usl.getId().equals("015") || t.usl.getId().equals("099")) {
                            log.info("***********!!!!!! dt={}, lsk={}, usl={} vol={} ar={} Kpr={}",
                                    Utl.getStrFromDate(curDt), t.kart.getLsk(), t.usl.getId(),
                                    t.vol.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    t.area.setScale(4, BigDecimal.ROUND_HALF_UP),
                                    t.kpr.setScale(4, BigDecimal.ROUND_HALF_UP));
                        }
                    }
                }
*/

            }
        }

    }

    private BigDecimal getParentVol(Map<String, UslPriceVolKart> mapUslPriceVol, BigDecimal naborNorm, String uslId) {
        BigDecimal dayVol = BigDecimal.ZERO;
        UslPriceVolKart uslPriceVolKart = mapUslPriceVol.get(uslId);
        if (uslPriceVolKart != null && !uslPriceVolKart.isMeter()) {
            // только если нет счетчика в родительской услуге
            // сложить все объемы родит.услуги, умножить на норматив текущей услуги
            dayVol = (uslPriceVolKart.getVol().add(uslPriceVolKart.getVolOverSoc()))
                    .multiply(naborNorm);
        }
        return dayVol;
    }

    /**
     * Построить объем для начисления
     *
     * @param curDt              - дата расчета
     * @param calcStore          - хранилище объемов
     * @param nabor              - строка набора
     * @param isLinkedEmpty      -
     * @param isLinkedExistMeter -
     * @param kartMain           - лиц.счет
     * @param detailUslPrice     - инф. о расценке
     * @param countPers          - инф. о кол.прожив.
     * @param socStandart        - соцнорма
     * @param isMeterExist       - наличие счетчика
     * @param dayVol             - объем
     * @param dayVolOverSoc      - объем свыше соц.нормы
     * @param kartArea           - площадь
     * @param areaOverSoc        - площадь свыше соц.нормы
     * @param isForChrg          - сохранять в начислении? (C_CHARGE)
     */
    private UslPriceVolKart buildVol(Date curDt, CalcStore calcStore, Nabor nabor, Boolean isLinkedEmpty,
                                     Boolean isLinkedExistMeter, Kart kartMain, DetailUslPrice detailUslPrice,
                                     CountPers countPers, SocStandart socStandart, boolean isMeterExist,
                                     BigDecimal dayVol, BigDecimal dayVolOverSoc, BigDecimal kartArea,
                                     BigDecimal areaOverSoc, boolean isForChrg) {
        return UslPriceVolKart.UslPriceVolBuilder.anUslPriceVol()
                .withDt(curDt)
                .withKart(nabor.getKart()) // группировать по лиц.счету из nabor!
                .withUsl(nabor.getUsl())
                .withUslOverSoc(detailUslPrice.uslOverSoc)
                .withUslEmpt(detailUslPrice.uslEmpt)
                .withOrg(nabor.getOrg())
                .withIsMeter(isLinkedExistMeter != null ? isLinkedExistMeter : isMeterExist)
                .withIsEmpty(isLinkedEmpty != null ? isLinkedEmpty : countPers.isEmpty)
                .withIsResidental(kartMain.isResidental())
                .withSocStdt(socStandart != null ? socStandart.norm : BigDecimal.ZERO)
                .withPrice(detailUslPrice.price)
                .withPriceOverSoc(detailUslPrice.priceOverSoc)
                .withPriceEmpty(detailUslPrice.priceEmpt)
                .withVol(dayVol)
                .withVolOverSoc(dayVolOverSoc)
                .withArea(kartArea)
                .withAreaOverSoc(areaOverSoc)
                .withKpr(countPers.kpr)
                .withKprOt(countPers.kprOt)
                .withKprWr(countPers.kprWr)
                .withKprNorm(countPers.kprNorm)
                .withPartDayMonth(calcStore.getPartDayMonth())
                .withIsForChrg(isForChrg)
                .build();
    }

    /**
     * Получить совокупное кол-во проживающих (родительский и дочерний лиц.счета)
     *
     * @param parVarCntKpr    - тип подсчета кол-во проживающих
     * @param parCapCalcKprTp - тип подсчета кол-во проживающих для капремонта
     * @param curDt           - дата расчета
     * @param nabor           - строка услуги
     * @param kartMain        - основной лиц.счет
     * @param isMeterExist    - наличие счетчика в расчетный день (работает только в КИС)
     */
    private CountPers getCountPersAmount(int parVarCntKpr, int parCapCalcKprTp,
                                         Date curDt, Nabor nabor, Kart kartMain, boolean isMeterExist) {
        CountPers countPers;
        countPers = kartPrMng.getCountPersByDate(kartMain, nabor,
                parVarCntKpr, parCapCalcKprTp, curDt, isMeterExist);

        if (nabor.getKart().getParentKart() != null) {
            // в дочернем лиц.счете (привязка)
            // для определения расценки по родительскому (если указан по parentKart) или текущему лиц.счету
            CountPers countPersParent = kartPrMng.getCountPersByDate(nabor.getKart().getParentKart(), nabor,
                    parVarCntKpr, parCapCalcKprTp, curDt, isMeterExist);
            countPers.kpr = countPersParent.kpr;
            countPers.isEmpty = countPersParent.isEmpty;

            if (parVarCntKpr == 0
                    && countPers.kprNorm == 0 &&
                    !kartMain.getStatus().getCd().equals("MUN")) {
                // вариант Кис.
                if (countPers.kprOt == 0) {
                    // в РСО счетах и кол-во временно отсут.=0
                    countPers.kprNorm = 1;
                } else {
                    if (Utl.in(nabor.getUsl().getFkCalcTp(), 17, 18, 19, 52, 53, 55, 56, 57)) {
                        if (isMeterExist) {
                            // х.в. г.в. водоотв., есть только ВО и только если счетчики
                            // (по сути здесь kprNorm ни на что не влияет, но решил заполнять)
                            // - КИС согласно ТЗ на 22.10.2019
                            countPers.kprNorm = countPers.kprOt;
                        }
                    }
                }
            }

        } else {
            // в родительском лиц.счете (может быть Основным, РСО и прочее, НЕ привязка)
            if (countPers.kprNorm == 0) {
                if (parVarCntKpr == 0) {
                    // Киселёвск
                    // ред. 24.05.2019
                    /* Алёна: Сделать начисление на 1 собственника по услугам 140, 016,012,014,
                        если квартира 0 и муниц.(у нас на 0 квартиры, на приват. начисляется сейчас).
                       Например адрес Багратиона 42-500, муниц.кв. с 0 прожив.
                       Начисление по данным услугам не ведется.*/
/*                    if (!kartMain.getStatus().getCd().equals("MUN")) {
                        // не муницип. помещение
                        if (nabor.getUsl().getFkCalcTp().equals(49)) {
                            // услуга по обращению с ТКО
                            countPers.kpr = 1;
                            countPers.kprNorm = 1;
                        } else if (countPers.kprOt == 0) {
                            countPers.kprNorm = 1;
                        } */

                    if (Utl.in(nabor.getUsl().getFkCalcTp(), 17, 18, 19, 52, 53, 55, 56, 57)) {
                        if (isMeterExist && countPers.kprOt >= 0) {
                            // х.в. г.в. водоотв., есть только ВО и только если счетчики
                            // (по сути здесь kprNorm ни на что не влияет, но решил заполнять)
                            // - КИС согласно ТЗ на 22.10.2019
                            countPers.kprNorm = countPers.kprOt;
                        } else if (countPers.kprOt == 0) {
                            countPers.kprNorm = 1;
                        }
                    } else if (nabor.getUsl().getFkCalcTp().equals(49)) {
                        // услуга по обращению с ТКО
                        countPers.kpr = 1;
                        countPers.kprNorm = 1;
                    } else if (countPers.kprOt == 0) {
                        // прочие услуги
                        countPers.kprNorm = 1;
                    }

                } else if (parVarCntKpr == 1 && countPers.kprOt == 0) {
                    // Полысаево
                    countPers.kprNorm = 1;
                }
            }
        }
        countPers.isEmpty = countPers.kpr == 0;
        if (nabor.getUsl().getFkCalcTp().equals(34)) {
            // по Повыш коэфф. для Полыс - не учитывается пустое-не пустое помещение
            countPers.isEmpty = false;
        }
        return countPers;
    }

    /**
     * Распределить объемы экономии по услугам лиц.счетов, рассчитанных по помещению
     *
     * @param chrgCountAmountLocal - локальное хранилище объемов по помещению
     * @param ko                   - помещение
     * @param lstSelUsl            - список ограничения услуг (например при распределении ОДН)
     */
    private synchronized void distODNeconomy(ChrgCountAmountLocal chrgCountAmountLocal,
                                             Ko ko, List<Usl> lstSelUsl) {
        // получить объемы экономии по всем лиц.счетам помещения
        List<ChargePrep> lstChargePrep = ko.getKart().stream()
                .flatMap(t -> t.getChargePrep().stream())
                .filter(c -> c.getTp().equals(4) && lstSelUsl.size() == 0)
                .filter(c -> c.getKart().getNabor().stream()
                        .anyMatch(n -> n.getUsl().equals(c.getUsl().getUslChild())
                                && n.isValid(true))) // только по действительным услугам ОДН
                .collect(Collectors.toList());

        // распределить экономию
        for (ChargePrep t : lstChargePrep) {
            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема в лиц.счете (когда были проживающие)
            List<UslVolKart> lstUslVolKart = chrgCountAmountLocal.getLstUslVolKart().stream()
                    .filter(d -> d.getKart().equals(t.getKart()) && d.getKprNorm().compareTo(BigDecimal.ZERO) != 0 && d.getUsl().equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета
            Utl.distBigDecimalByList(t.getVol(), lstUslVolKart, 5);

            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема во вводе (когда были проживающие)
            List<UslVolVvod> lstUslVolVvod = chrgCountAmountLocal.getLstUslVolVvod().stream()
                    .filter(d -> d.getKprNorm().compareTo(BigDecimal.ZERO) != 0 && d.getUsl().equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов ввода
            Utl.distBigDecimalByList(t.getVol(), lstUslVolVvod, 5);

            // РАСПРЕДЕЛИТЬ по датам, детально
            // в т.ч. для услуги calcTp=47 (Тепл.энергия для нагрева ХВС (Кис.)) (когда были проживающие)
            // в т.ч. по услугам х.в. и г.в. (для водоотведения)
            List<UslPriceVolKart> lstUslPriceVolKart = chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                    .filter(d -> d.getKart().equals(t.getKart()) && d.getKprNorm().compareTo(BigDecimal.ZERO) != 0
                            && d.getUsl().equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета, по датам
            Utl.distBigDecimalByList(t.getVol(), lstUslPriceVolKart, 5);

            // ПО СГРУППИРОВАННЫМ объемам до лиц.счетов, просто снять объем
            chrgCountAmountLocal.getLstUslVolKartGrp().stream()
                    .filter(d -> d.getKart().equals(t.getKart()) && d.getUsl().equals(t.getUsl()))
                    .findFirst().ifPresent(uslVolKartGrp -> uslVolKartGrp.setVol(uslVolKartGrp.getVol().add(t.getVol())));
        }
    }
}