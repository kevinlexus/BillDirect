package com.dic.app.mm;

import com.ric.cmn.excp.WrongParam;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface ThreadMng<T> {

	void invokeThreads(PrepThread<T> reverse, int cntThreads, List<T> lstItem, String string)
			throws InterruptedException, ExecutionException, WrongParam;
}
