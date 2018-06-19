package com.dic.app.mm;

import java.util.concurrent.Future;

import com.ric.dto.Result;


public interface ComprTbl {

	public Future<Result> comprTableByLsk(String table, String lsk, Integer backPeriod, Integer curPeriod, boolean isAllPeriods, boolean isByUsl);

}