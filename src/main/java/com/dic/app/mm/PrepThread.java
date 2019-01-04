package com.dic.app.mm;

import java.util.concurrent.Future;

import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.CommonResult;

public interface PrepThread<T> {

	Future<CommonResult> lambdaFunction(T itemWork, double proc) throws WrongParam, ErrorWhileChrg;

}

