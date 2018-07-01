package com.dic.app.mm;

import java.util.concurrent.Future;

import com.ric.dto.CommonResult;

public interface PrepThread<T> {

	Future<CommonResult> lambdaFunction(T itemWork, double proc);

}

