package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.*;
import com.ric.cmn.excp.*;
import com.ric.dto.CommonResult;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface ProcessMng {

    String processWebRequest(int tp, int debugLvl, Date genDt, House house, Vvod vvod, Ko ko, Org uk, Usl usl);

    void processAll(RequestConfigDirect reqConf) throws ErrorWhileGen;

    CompletableFuture<CommonResult> process(RequestConfigDirect reqConf) throws ErrorWhileGen;
}
