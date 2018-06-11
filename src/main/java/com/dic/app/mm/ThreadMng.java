package com.dic.app.mm;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface ThreadMng<T> {

	void invokeThreads(PrepThread<T> reverse, int cntThreads, List<T> lstItem, boolean isCheckStop)
			throws InterruptedException, ExecutionException;
}
