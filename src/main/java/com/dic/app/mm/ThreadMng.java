package com.dic.app.mm;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.dic.bill.dto.CalcStore;

public interface ThreadMng<T> {

	void invokeThreads(PrepThread<T> reverse, CalcStore calcStore, int cntThreads, List<T> lstItem) throws InterruptedException, ExecutionException;

	//void invokeThreads(CalcStore calcStore, int cntThreads, List<T> lstItem);

}
