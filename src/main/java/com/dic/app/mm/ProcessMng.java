package com.dic.app.mm;

import java.util.concurrent.Future;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.*;
import com.ric.dto.CommonResult;

public interface ProcessMng {

    void distVol(RequestConfig reqConf)
            throws ErrorWhileGen;

    void genProcessAll(RequestConfig reqConf, CalcStore calcStore) throws ErrorWhileGen;

    CalcStore buildCalcStore(RequestConfig reqConf);

    Future<CommonResult> genProcess(long klskId, CalcStore calcStore, RequestConfig reqConf) throws WrongParam, ErrorWhileChrg;
}
