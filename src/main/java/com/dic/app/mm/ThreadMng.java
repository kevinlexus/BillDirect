package com.dic.app.mm;

import java.util.List;

import com.dic.bill.dto.CalcStore;

public interface ThreadMng<T> {

	void invokeThreads(PrepThread<T> reverse, CalcStore calcStore, int cntThreads, List<T> lstItem);

	//void invokeThreads(CalcStore calcStore, int cntThreads, List<T> lstItem);

}
