package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.DistPayMng;
import com.dic.app.mm.DistPayQueueMng;
import com.dic.bill.dto.KwtpMgRec;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Сервис очередей распределения оплаты
 *
 * @version 1.00
 */
@Slf4j
@Service
@Scope("prototype")
public class DistPayQueueMngImpl implements DistPayQueueMng {

    private final DistPayMng distPayMng;

    @PersistenceContext
    private EntityManager em;
    private final ConfigApp config;

    private final List<KwtpMgRec> lstKwtpMgRec = new ArrayList<>();
    private volatile Boolean isProcessDist = false;

    public DistPayQueueMngImpl(DistPayMng distPayMng, EntityManager em, ConfigApp config) {
        this.distPayMng = distPayMng;
        this.em = em;
        this.config = config;
    }

    /**
     * Добавить платеж в очередь на распределение
     *
     * @param kwtpMgRec - строка KwtpMg
     */
    @Override
    public void queueKwtpMg(KwtpMgRec kwtpMgRec) {
        synchronized (lstKwtpMgRec) {
            lstKwtpMgRec.add(kwtpMgRec);
        }
    }


    /**
     * Обработка очереди платежей многопоточно
     */
    @Scheduled(fixedDelay = 1000)
    private void processQueue() {
        // кол-во потоков
        final int cntThreads = 10;
        // кол-во записей для обработки на поток
        final int cntFetch = 3;

        int lstSize;
        synchronized (lstKwtpMgRec) {
            lstSize = lstKwtpMgRec.size();
        }

        if (lstSize > 0) {
            synchronized (isProcessDist) {
            if (!isProcessDist) {
                isProcessDist = true;
                // есть платежи на обработку
                log.info("Process Queue");

                // создание потоков с и спользованием пула потоков
                ExecutorService threadPool = Executors.newFixedThreadPool(cntThreads,
                        new CustomizableThreadFactory("DistPayQueueMng-"));
                // защелка выполнения потоков
                CountDownLatch latch = new CountDownLatch(cntThreads);

                for (int i = 1; i <= cntThreads; i++) {
                    threadPool.submit(() ->
                    {
                        boolean isStop = false;
                        while (!isStop) {
                            List<KwtpMgRec> lst = getNext(cntFetch);
                            if (lst != null) {
                                for (KwtpMgRec t : lst) {
                                    try {
                                        distPayMng.distKwtpMg(t.getKwtpMgId(), t.getLsk(),
                                                t.getStrSumma(), t.getStrPenya(), t.getStrDebt(),
                                                t.getDopl(), t.getNink(), t.getNkom(), t.getOper(),
                                                t.getStrDtek(), t.getStrDatInk(), t.isTest());
                                    } catch (ErrorWhileDistPay e) {
                                        log.error("ERROR! Ошибка в процессе распределения средств по KwtpMg.Id={}", t.getKwtpMgId());
                                        log.error(Utl.getStackTraceString(e));
                                    }
                                }
                            } else {
                                isStop = true;
                            }
                        }
                        latch.countDown();
                    });
                }

                // ожидание окончания потоков
                try {
                    latch.await();
                    threadPool.shutdown();
                } catch (InterruptedException ex) {
                    log.error("ERROR! Ошибка в потоке распределения платежей - остановка пула потоков");
                    threadPool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                isProcessDist = false;
            }
            }
        }
    }

    /**
     * Получить следующие cnt записей платежей для обработки
     *
     * @param cnt - кол-во записей
     */
    private List<KwtpMgRec> getNext(int cnt) {

        synchronized (lstKwtpMgRec) {
            if (lstKwtpMgRec.size() > 0) {
                List<KwtpMgRec> lst2 = new ArrayList<>();
                Iterator<KwtpMgRec> itr = lstKwtpMgRec.iterator();
                for (int i = 1; i <= cnt; i++) {
                    if (itr.hasNext()) {
                        lst2.add(itr.next());
                        itr.remove();
                    }
                }
                return lst2;
            } else {
                return null;
            }

        }
    }

}
