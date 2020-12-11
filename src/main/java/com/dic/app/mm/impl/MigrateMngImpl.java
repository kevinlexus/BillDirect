package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.*;
import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Сервис для миграции данных в другие структуры
 *
 * @author Lev
 */
@Slf4j
@Service
public class MigrateMngImpl implements MigrateMng {

    @PersistenceContext
    private EntityManager em;
    private final ConfigApp config;
    private final ApplicationContext ctx;
    private final ThreadMng<String> threadMng;
    private final MigrateUtlMng migUtlMng;

    public MigrateMngImpl(ConfigApp config, ApplicationContext ctx, ThreadMng<String> threadMng, MigrateUtlMng migUtlMng) {
        this.config = config;
        this.ctx = ctx;
        this.threadMng = threadMng;
        this.migUtlMng = migUtlMng;
    }


    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void migrateAll(String lskFrom, String lskTo, Integer dbgLvl) throws ErrorWhileGen {
        long startTime = System.currentTimeMillis();
        log.info("НАЧАЛО миграции задолженности в новые структуры");
        log.info("ВНИМАНИЕ! используется A_PENYA предыдущего периода");
        log.info("формировать ничего не надо!");
        // построить запрос
        RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                .withTp(5)
                .withLskFrom(lskFrom)
                .withLskTo(lskTo)
                .withCurDt1(config.getCurDt1())
                .withCurDt2(config.getCurDt2())
                .withRqn(config.incNextReqNum())
                .withIsMultiThreads(true)
                .build();
        reqConf.prepareId();
        StopWatch sw = new StopWatch();
        sw.start("TIMING: " + reqConf.getTpName());

        ProcessMng processMng = ctx.getBean(ProcessMng.class);
        processMng.processAll(reqConf);

        sw.stop();
        System.out.println(sw.prettyPrint());
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.info("ОКОНЧАНИЕ миграции задолженности - Общее время выполнения={}", totalTime);

    }

    /**
     * Перенести данные из таблиц Директ, в систему учета долгов
     * по услуге, организации, периоду
     *
     * @param lsk        - лицевой счет
     * @param periodBack - как правило предыдущий период, относительно текущего
     * @param period     - текущий период
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void migrateDeb(String lsk, Integer periodBack, Integer period, Integer dbgLvl) {

        log.info("НАЧАЛО РАСПРЕДЕЛЕНИЯ лиц.счета={}", lsk);
        // получить задолженность
        List<SumDebMgRec> lstDeb = migUtlMng.getDeb(lsk, periodBack);

        // получить исходящее сальдо предыдущего периода
        List<SumDebUslMgRec> lstSal = migUtlMng.getSal(lsk, period);
        log.info("Итого Сальдо={}", lstSal.stream().map(SumDebUslMgRec::getSumma).reduce(BigDecimal.ZERO, BigDecimal::add));


        // получить начисление по услугам и орг., по всем необходимым периодам задолжности
        List<SumDebUslMgRec> lstChrg = migUtlMng.getChrg(lsk, lstDeb, lstSal);
        if (dbgLvl.equals(1)) {
            // распечатать начисление
            log.info("*** НАЧИСЛЕНИЕ, по лс {}:", lsk);
            migUtlMng.printChrg(lstChrg);
        }
        // распечатать долг
        //printDeb(lstDeb);
        // распечатать начисление
        //log.info("*** НАЧИСЛЕНИЕ:");
        //migUtlMng.printChrg(lstChrg);
        //log.info("");

        // РАСПРЕДЕЛЕНИЕ
        // результат распределения
        List<SumDebUslMgRec> lstDebResult = new ArrayList<>();

        log.info("*** ДОЛГ до распределения, по лс {}:", lsk);
        migUtlMng.printDeb(lstDeb);
        migUtlMng.printSal(lstSal);
        log.info("");

        // получить тип долга
        int debTp = migUtlMng.getDebTp(lstDeb);
        if (debTp == 1 || debTp == -1) {
            // только задолженности или только переплаты

            // распределить сперва все положительные или отрицательные числа
            // зависит от типа задолженности
            log.info("*** РАСПРЕДЕЛИТЬ долги одного знака, по sign={}", debTp);
            boolean res = distSalByDeb(lstDeb, lstSal, lstChrg, lstDebResult, debTp, true, dbgLvl);
            if (!res) {
                // не удалось распределить, распределить принудительно
                // добавив нужный период в строку с весом 1.00 руб, в начисления
                migUtlMng.addSurrogateChrg(lstDeb, lstSal, lstChrg, debTp);
                // вызвать еще раз распределение, не устанавливая веса
                distSalByDeb(lstDeb, lstSal, lstChrg, lstDebResult, debTp, false, dbgLvl);
            }

            if (dbgLvl.equals(1)) {
                // распечатать долг
                migUtlMng.printDeb(lstDeb);
                migUtlMng.printSal(lstSal);
            }
        } else if (debTp == 0) {
            // смешанные долги
            log.info("*** РАСПРЕДЕЛИТЬ смешанные суммы долги и перплаты");

            // распределить сперва все ДОЛГИ
            int sign = 1;
            log.info("*** РАСПРЕДЕЛИТЬ сперва ДОЛГИ");
            boolean res = distSalByDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, true, dbgLvl);
            if (!res) {
                // не удалось распределить, распределить принудительно
                // добавив нужный период в строку с весом 1.00 руб, в начисления
                migUtlMng.addSurrogateChrg(lstDeb, lstSal, lstChrg, sign);
                // вызвать еще раз распределение, не устанавливая веса
                distSalByDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, false, dbgLvl);
            }
            if (dbgLvl.equals(1)) {
                // распечатать долг
                migUtlMng.printDeb(lstDeb);
                migUtlMng.printSal(lstSal);
            }

            // распределить все ПЕРЕПЛАТЫ
            sign = -1;
            log.info("*** РАСПРЕДЕЛИТЬ ПЕРЕПЛАТЫ");
            res = distSalByDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, true, dbgLvl);
            if (!res) {
                // не удалось распределить, распределить принудительно
                // добавив нужный период в строку с весом 1.00 руб, в начисления
                migUtlMng.addSurrogateChrg(lstDeb, lstSal, lstChrg, sign);
                // вызвать еще раз распределение, не устанавливая веса
                distSalByDeb(lstDeb, lstSal, lstChrg, lstDebResult, sign, false, dbgLvl);
            }
            if (dbgLvl.equals(1)) {
                // распечатать долг
                migUtlMng.printDeb(lstDeb);
                migUtlMng.printSal(lstSal);
            }
        }

        if (dbgLvl.equals(1)) {
            log.info("*** ДОЛГ ДО СЛОЖЕНИЯ, по лс {}:", lsk);
            migUtlMng.printDebResult(lstDebResult);
        }

        // на данном этапе должны идти суммы распределения
        migUtlMng.checkSumma(lstSal, lstDeb, lsk);

        // сложить дебет и кредит
        groupResult(lstDebResult);

        // проверить наличие распределения
        Cnt cnt = new Cnt();
        migUtlMng.check(lstSal, lstDeb, cnt);

        log.info("*** cntSal={}, cntDeb={}", cnt.cntSal, cnt.cntDeb);
        if (cnt.cntSal != 0L && cnt.cntDeb != 0L) {
            // сальдо не распределено, долги не распределены
            migUtlMng.distDebFinal(periodBack, lstDebResult, cnt, lsk);
            migUtlMng.distSalFinal(periodBack, lstDebResult, cnt);
        } else if (cnt.cntSal == 0L && cnt.cntDeb != 0L) {
            // сальдо распределено, долги не распределены
            // вызвать принудительное распределение
            log.info("*** РАСПРЕДЕЛИТЬ ДОЛГИ финально");
            migUtlMng.distDebFinal(periodBack, lstDebResult, cnt, lsk);

        } else if (cnt.cntSal != 0L) {
            // сальдо не распределено, долги распределены
            log.info("*** РАСПРЕДЕЛИТЬ САЛЬДО финально");
            migUtlMng.distSalFinal(periodBack, lstDebResult, cnt);
        }

        // проверить наличие распределения
        migUtlMng.check(lstSal, lstDeb, cnt);

        if (cnt.cntDeb != 0L) {
            // долги не распределены
            throw new RuntimeException("ОШИБКА #1 не распределены ДОЛГИ в лс=" + lsk);
        }
        if (cnt.cntSal != 0L) {
            // сальдо не распределено
            throw new RuntimeException("ОШИБКА #2 не распределено САЛЬДО в лс=" + lsk);
        }

        log.info("");
        log.info("*** ДОЛГ после распределения, по лс {}:", lsk);
        migUtlMng.printDeb(lstDeb);
        migUtlMng.printSal(lstSal);
        migUtlMng.printDebResult(lstDebResult);


        // удалить предыдущее распределение
        //debDao.delByLskPeriod(lsk, periodBack);
        // сохранить распределённые задолженности
        Kart kart = em.find(Kart.class, lsk);

        for (SumDebUslMgRec t : lstDebResult) {
            Usl usl = em.find(Usl.class, t.getUslId());
            if (usl == null) {
                throw new RuntimeException("ОШИБКА #4 сохранения задолженности, не найдена услуга usl=" + t.getUslId());
            }
            Org org = em.find(Org.class, t.getOrgId());
            if (org == null) {
                throw new RuntimeException("ОШИБКА #5 сохранения задолженности, не найдена организация org=" + t.getOrgId());
            }
            // сохранить новое, если не ноль
/*
            if (t.getSumma().compareTo(BigDecimal.ZERO) !=0) {
                Deb deb = Deb.builder()
                        .withKart(kart)
                        .withDebOut(t.getSumma())
                        .withMg(t.getMg())
                        .withUsl(usl)
                        .withOrg(org)
                        .withMgFrom(periodBack)
                        .withMgTo(periodBack)
                        .build();
                em.persist(deb);
            }
*/
        }

        log.info("ОКОНЧАНИЕ РАСПРЕДЕЛЕНИЯ лиц.счета={}", lsk);
        log.info("");
        CommonResult res = new CommonResult(0, 0);
    }

    /**
     * Сложить дебет и кредит
     */
    private void groupResult(List<SumDebUslMgRec> lstDebResult) {
        // найти положительные, сложить с отрицательными
        lstDebResult.stream()
                .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                .filter(t -> t.getSign().equals(1))
                .forEach(t -> {
                    SumDebUslMgRec found =
                            lstDebResult.stream()
                                    .filter(d -> d.getSign().equals(-1))
                                    .filter(d -> d.getUslId().equals(t.getUslId()) && d.getOrgId().equals(t.getOrgId()))
                                    .findFirst().orElse(null);
                    if (found != null) {
                        t.setSumma(t.getSumma().add(found.getSumma().multiply(BigDecimal.valueOf(-1))));
                        found.setSumma(BigDecimal.ZERO);
                    }
                });
        // найти отрицательные, сложить с положительными
        lstDebResult.stream()
                .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                .filter(t -> t.getSign().equals(-1))
                .forEach(t -> {
                    SumDebUslMgRec found =
                            lstDebResult.stream()
                                    .filter(d -> d.getSign().equals(1))
                                    .filter(d -> d.getUslId().equals(t.getUslId()) && d.getOrgId().equals(t.getOrgId()))
                                    .findFirst().orElse(null);
                    if (found != null) {
                        t.setSumma((t.getSumma().multiply(BigDecimal.valueOf(-1))).add(found.getSumma()));
                        found.setSumma(BigDecimal.ZERO);
                    } else {
                        // промаркировать минусом
                        t.setSumma(t.getSumma().multiply(BigDecimal.valueOf(-1)));
                    }
                });
    }

    /**
     * Распределить сальдо по долгам
     *
     * @param lstDeb       - долги по периодам
     * @param lstSal       - сальдо по усл.+орг.
     * @param lstChrg      - начисления по периодам
     * @param lstDebResult - результат
     * @param sign         - знак распределения
     * @param isSetWeigths - повторно установить веса?
     */
    private boolean distSalByDeb(List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal, List<SumDebUslMgRec> lstChrg,
                                 List<SumDebUslMgRec> lstDebResult, int sign, boolean isSetWeigths, Integer dbgLvl) {
        BigDecimal sumAmnt = BigDecimal.ZERO;  // сумма для логгирования распределения
        if (isSetWeigths) {
            // установить веса по начислению
            migUtlMng.setWeigths(lstSal, lstChrg, sign);
        }
        // продолжать цикл распределение?
        boolean isContinue = true;
        // не может распределиться?
        boolean isCantDist = false;
        while (isContinue) {
            isContinue = false;
            BigDecimal sumDistAmnt = BigDecimal.ZERO;
            // перебирать сальдо
            for (SumDebUslMgRec t : lstSal.stream()
                    .filter(t -> t.getSign().equals(sign)) // знак долга
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
                    .collect(Collectors.toList())
            ) {
                // распределить, добавить сумму
                sumDistAmnt = sumDistAmnt.add(
                        distSalByPeriodDeb(t, lstDeb, lstChrg, lstDebResult, sign, dbgLvl)
                );
                // продолжать
                isContinue = true;
            }

            sumAmnt = sumAmnt.add(sumDistAmnt);
            //log.info("sumAmnt={}", sumAmnt);

            if (sumDistAmnt.compareTo(BigDecimal.ZERO) == 0) {
                // не продолжать, не распределяется
                isContinue = false;
                isCantDist = true;
            }
        }
        return !isCantDist;
    }

    /**
     * Распределить сумму сальдо по долгу
     *
     * @param sal          - запись сальдо
     * @param lstDeb       - задолженности
     * @param lstChrg      - начисление
     * @param lstDebResult - результат долгов
     * @param sign         - знак распределения
     * @param dbgLvl       - уровень отладки
     */
    private BigDecimal distSalByPeriodDeb(SumDebUslMgRec sal, List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstChrg,
                                          List<SumDebUslMgRec> lstDebResult, int sign, Integer dbgLvl) {
        BigDecimal sumAmnt = BigDecimal.ZERO;
        // получить строки начисления в которых работали данные услуга + орг
        // распределить по всем периодам
        List<SumDebUslMgRec> lstChrgPeriod = migUtlMng.getPeriod(sal.getUslId(), sal.getOrgId(), lstChrg);
        for (SumDebUslMgRec t : lstChrgPeriod) {
            BigDecimal summaDist;
            BigDecimal weigth = t.getWeigth();
            if (weigth.compareTo(sal.getSumma()) <= 0) {
                // сумма веса меньше или равна сумме сальдо
                summaDist = t.getWeigth();
            } else {
                // взять всю оставшуюся сумму сальдо
                summaDist = sal.getSumma();
            }

            // найти в долгах период для распределения
            SumDebMgRec foundDeb;
            foundDeb = lstDeb.stream()
                    .filter(d -> d.getMg().equals(t.getMg()))
                    .filter(d -> d.getSign().equals(sign)) // знак распределения
                    .filter(d -> d.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
                    .findAny().orElse(null);
            BigDecimal foundDebSumma;
            if (foundDeb != null) {
                foundDebSumma = foundDeb.getSumma();
                if (summaDist.compareTo(foundDebSumma) > 0) {
                    // сумма для распределения больше суммы долга
                    // взять всю оставшуюся сумму долга
                    summaDist = foundDebSumma;
                }
                // уменьшить сумму сальдо
                sal.setSumma(sal.getSumma().subtract(summaDist));
                // уменьшить сумму долга
                foundDeb.setSumma(foundDeb.getSumma().subtract(summaDist));
                // записать результат
                migUtlMng.insDebResult(lstDebResult, foundDeb.getMg(), sal.getUslId(), sal.getOrgId(), summaDist, sign);
                sumAmnt = sumAmnt.add(summaDist);
/*				if (sal.getUslId().equals("016") && sal.getOrgId().equals(708)) {
					log.info("############ добавлено: mg={}, uslId={}, orgId={}, summaDist={}", foundDeb.getMg(), sal.getUslId(), sal.getOrgId(), summaDist);
				}
*/
            }
        }

        if (sumAmnt.compareTo(BigDecimal.ZERO) > 0) {
            // сумма распределяется
            return sumAmnt;
        } else {
            // сумма не может распределиться найти прочие орг. + усл., чтобы вытеснить на другие периоды
	/*		log.info("Не может распр. сумма={}, знак={}, uslId={}, orgId={}, попытка переместить другие усл.+орг. в прочие периоды",
					sal.getChrg(), sal.getSign(), sal.getUslId(), sal.getOrgId());

			migUtlMng.printDeb(lstDeb);
			migUtlMng.printChrg(lstChrg);
			migUtlMng.printDebResult(lstDebResult);
	*/

            for (SumDebUslMgRec t : lstChrgPeriod) {
                BigDecimal summaDist;
                BigDecimal weigth = t.getWeigth();
                if (weigth.compareTo(sal.getSumma()) <= 0) {
                    // сумма веса меньше или равна сумме сальдо
                    summaDist = t.getWeigth();
                } else {
                    // взять всю оставшуюся сумму сальдо
                    summaDist = sal.getSumma();
                }

                // найти в долгах период для распределения
                SumDebMgRec foundDeb;
                foundDeb = lstDeb.stream()
                        .filter(d -> d.getMg().equals(t.getMg()))
                        .filter(d -> d.getSign().equals(sign)) // знак распределения
                        .findAny().orElse(null);
                if (foundDeb != null) {
                    // втиснуть сумму
                    BigDecimal summaIns = pushDebResult(summaDist, t.getMg(), t.getUslId(),
                            t.getOrgId(), sign, lstDeb, lstChrg, lstDebResult, dbgLvl);
                    if (summaIns != null) {
/*						if (sal.getUslId().equals("016") && sal.getOrgId().equals(708)) {
							log.info("############ втиснуто: mg={}, uslId={}, orgId={}, summaDist={}",
									t.getMg(), t.getUslId(), t.getOrgId(), summaIns);
						}
*/                        // уменьшить сумму сальдо
                        sal.setSumma(sal.getSumma().subtract(summaIns));
                        // сумма распределяется
                        sumAmnt = sumAmnt.add(summaIns);
                    }
                }
            }
            // вернуть, сумму, сколько распределено
            return sumAmnt;
        }
    }

    /**
     * Перенести распределенные долг по усл. + орг. на другие допустимые периоды
     *
     * @param summa        - сумма для перемещения
     * @param period       - период
     * @param uslId        - код услуги
     * @param orgId        - код организации
     * @param sign         - знак распределения
     * @param lstDeb       - долги
     * @param lstChrg      - начисления
     * @param lstDebResult - результат долгов
     * @return вернуть фактически проведённую сумму
     */
    private BigDecimal pushDebResult(BigDecimal summa, Integer period, String uslId, Integer orgId,
                                     int sign, List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstChrg,
                                     List<SumDebUslMgRec> lstDebResult, Integer dbgLvl) {
        // найти в результатах долгов
        List<SumDebUslMgRec> lst = lstDebResult.stream()
                .filter(t -> !t.getUslId().equals(uslId) || !t.getOrgId().equals(orgId)) // прочие усл. или орг.
                .filter(t -> t.getMg().equals(period)) // данный период
                .filter(t -> t.getSign().equals(sign)) // данный знак
                .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
                .collect(Collectors.toList());
        for (SumDebUslMgRec srcDebResult : lst) {
            //log.info("найдено в результатах долгов usl={}, org={}, mg={}", srcDeb.getUslId(), srcDeb.getOrgId(), srcDeb.getMg());
            // найти такие же усл.+орг. в другом периоде начислений
            List<SumDebUslMgRec> lstOtherChrg = lstChrg.stream()
                    .filter(t -> t.getUslId().equals(srcDebResult.getUslId()) && t.getOrgId().equals(srcDebResult.getOrgId()))
                    .filter(t -> !t.getMg().equals(srcDebResult.getMg()))
                    .collect(Collectors.toList());
            for (SumDebUslMgRec d : lstOtherChrg) {
                // найти еще нераспределенные долги
                SumDebMgRec nonDistDeb = lstDeb.stream()
                        .filter(t -> t.getMg().equals(d.getMg()) // с таким периодом как в начислении
                                && t.getSumma().compareTo(BigDecimal.ZERO) > 0) // ненулевые
                        .filter(t -> t.getSign().equals(sign)) // данный знак
                        .findFirst().orElse(null);
                if (nonDistDeb != null) {
                    // найдено
                    BigDecimal summaDist;
                    if (summa.compareTo(srcDebResult.getSumma()) > 0) {
                        // сумма для распределения больше чем уже распр.долг по прочей усл. + орг.
                        // взять этот распр.долг
                        summaDist = srcDebResult.getSumma();
                        //log.info("check1");
                    } else {
                        // взять сумму распр.
                        summaDist = summa;
                        //log.info("check2 val1={} val2={} is={}", summa, srcDebResult.getChrg(), summa.compareTo(srcDebResult.getChrg()) > 0);
                    }

                    if (summaDist.compareTo(nonDistDeb.getSumma()) > 0) {
                        // сумма для распределения больше суммы еще не распр.долга
                        // взять всю оставшуюся сумму долга
                        summaDist = nonDistDeb.getSumma();
                    }

                    // перенести сумму распр. долга в другой период
                    srcDebResult.setSumma(srcDebResult.getSumma().subtract(summaDist));
                    nonDistDeb.setSumma(nonDistDeb.getSumma().subtract(summaDist));
						/*if (srcDebResult.getChrg().compareTo(BigDecimal.ZERO) < 0) {
							log.info("stop1!");
						}
						if (srcDebResult.getChrg().compareTo(BigDecimal.ZERO) < 0) {
							log.info("stop2!");
						}*/
                    // поставить сумму долга в результате на другой период
                    migUtlMng.insDebResult(lstDebResult, nonDistDeb.getMg(), srcDebResult.getUslId(), srcDebResult.getOrgId(), summaDist, sign);
                    // записать результат распределения
                    migUtlMng.insDebResult(lstDebResult, period, uslId, orgId, summaDist, sign);
                    if (dbgLvl.equals(1)) {
                        log.info("summa={}, sign={}, uslId={}, orgId={} mg={}, не поместилась в период, вызвано перемещение:",
                                summa, sign, uslId, orgId, period);
                        log.info("summa={}, uslId={}, orgId={} mg={}, перенесена в период mg={}",
                                summaDist, srcDebResult.getUslId(), srcDebResult.getOrgId(), srcDebResult.getMg(), nonDistDeb.getMg());
                    }
                    return summaDist;
                }
            }

        }
        // не найдено ничего
        return null;
    }


}