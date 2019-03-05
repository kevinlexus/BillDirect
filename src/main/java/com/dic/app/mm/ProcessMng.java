package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.*;
import com.ric.dto.CommonResult;

import java.util.Date;
import java.util.concurrent.Future;

public interface ProcessMng {

   // void distVolAll(RequestConfigDirect reqConf)
//            throws ErrorWhileGen;

    void genProcessAll(RequestConfigDirect reqConf) throws ErrorWhileGen;

    Future<CommonResult> genProcess(RequestConfigDirect reqConf) throws ErrorWhileGen;
}
