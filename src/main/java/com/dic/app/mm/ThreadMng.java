package com.dic.app.mm;

import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface ThreadMng<T> {

	void invokeThreads(PrepThread<T> reverse, int cntThreads, List<T> lstItem, boolean isCheckStop, int rqn, String stopMark)
            throws InterruptedException, ExecutionException, WrongParam, ErrorWhileChrg;
}
