package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.PrepThread;
import com.dic.app.mm.ProcessMng;
import com.dic.app.mm.ThreadMng;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Сервис создания потоков
 *
 * @author Lev
 * @version 1.00
 */
@Slf4j
@Service
public class ThreadMngImpl<T> implements ThreadMng<T> {

    @Autowired
    private ApplicationContext ctx;
    @PersistenceContext
    private EntityManager em;
    @Autowired
    private ConfigApp config;

    /**
     * Вызвать выполнение потоков распределения объемов/ начисления - новый метод
     * @param reqConf - кол-во потоков
     * @param rqn - номер запроса
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void invokeThreads(RequestConfigDirect reqConf, int rqn)
            throws ErrorWhileGen {
        log.info("Будет создано {} потоков", reqConf.getCntThreads());
        List<CompletableFuture<CommonResult>> lst = new ArrayList<>();
        for (int i = 0; i < reqConf.getCntThreads(); i++) {
            // создать новый поток, передать информацию о % выполнения
            //log.info("********* Создан новый поток-1 tpName={}", reqConf.getTpName());
            ProcessMng processMng = ctx.getBean(ProcessMng.class);
            //log.info("********* Создан новый поток-2 tpName={}", reqConf.getTpName());
            CompletableFuture<CommonResult> ret = processMng.process(reqConf);
            //log.info("********* Создан новый поток-3 tpName={}", reqConf.getTpName());
            lst.add(ret);
            //log.info("********* Создан новый поток-4 tpName={}", reqConf.getTpName());
        }

        // ждать потоки
        lst.forEach(CompletableFuture::join);

    }


    /**
     * Вызвать выполнение потоков распределения объемов/ начисления - старый метод
     * @param reverse -   lambda функция
     * @param cntThreads - кол-во потоков
     * @param lstItem    - список Id на обработку
     * @param isCheckStop - проверять остановку главного процесса?
     * @param rqn - номер запроса
     * @param stopMark - маркер остановки процесса
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void invokeThreads(PrepThread<T> reverse,
                              int cntThreads, List<T> lstItem, boolean isCheckStop, int rqn, String stopMark)
            throws InterruptedException, ExecutionException, WrongParam, ErrorWhileChrg, ErrorWhileGen {
        log.info("Будет создано {} потоков", cntThreads);
        long startTime = System.currentTimeMillis();
        // размер очереди
        int lstSize = lstItem.size();
        int curSize = lstSize;
        List<Future<CommonResult>> frl = new ArrayList<>(cntThreads);
        for (int i = 1; i <= cntThreads; i++) {
            frl.add(null);
        }
        // проверить окончание всех потоков и запуск новых потоков
        T itemWork;
        boolean isStop = false;
        // флаг принудительной остановки
        boolean isStopProcess = false;
        while (!isStop && !isStopProcess) {
            Future<CommonResult> fut;
            int i = 0;
            // флаг наличия потоков
            isStop = true;
            for (Future<CommonResult> aFrl : frl) {
                if (isCheckStop && config.getLock().isStopped(stopMark)) {
                    // если процесс был остановлен, выход
                    isStopProcess = true;
                    break;
                }

                fut = aFrl;
                if (fut == null) {
                    // получить новый объект
                    itemWork = getNextItem(lstItem);
                    // уменьшить кол-во на 1
                    curSize = curSize - 1;
                    // рассчитать процент выполнения
                    double proc = 0;
                    if (lstSize > 0) {
                        proc = (1 - (double) curSize / (double) lstSize);
                    }
                    if (itemWork != null) {
                        // создать новый поток, передать информацию о % выполнения
                        log.info("********* Создан новый поток по itemWork={}", itemWork);
                        fut = reverse.lambdaFunction(itemWork, proc);
                        frl.set(i, fut);
                    }
                } else {
                    // не удалять! отслеживает ошибку в потоке!
                    try {
                        if (fut.get().getErr() == 1) {
                        }
                    } catch (Exception e) {
                        log.error(Utl.getStackTraceString(e));
                        log.error("ОШИБКА ПОСЛЕ ЗАВЕРШЕНИЯ ПОТОКА, ВЫПОЛНЕНИЕ ОСТАНОВКИ ПРОЧИХ ПОТОКОВ!");
                        config.getLock().unlockProc(rqn, stopMark);
                    }
                    // очистить переменную потока
                    frl.set(i, null);
                }

                if (fut != null) {
                    // не завершен поток
                    isStop = false;
                }
                i++;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error(Utl.getStackTraceString(e));
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        if (lstItem.size() > 0) {
            log.info("Итоговое время выполнения одного {} cnt={}, мс."
                    , totalTime / lstItem.size());
        }
    }

    // получить следующий объект, для расчета в потоках
    private T getNextItem(List<T> lstItem) {
        Iterator<T> itr = lstItem.iterator();
        T item = null;
        if (itr.hasNext()) {
            item = itr.next();
            itr.remove();
        }

        return item;
    }


}