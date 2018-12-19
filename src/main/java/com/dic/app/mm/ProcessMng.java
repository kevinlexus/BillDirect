package com.dic.app.mm;

import java.util.Date;
import java.util.concurrent.Future;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.CommonResult;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface ProcessMng {

    void genProcessAll(String lskFrom, String lskTo, Date genDt, Integer debugLvl,
                       RequestConfig reqConf) throws ErrorWhileChrgPen;

    CalcStore buildCalcStore(Date genDt, Integer debugLvl);

    Future<CommonResult> genProcess(String lsk, CalcStore calcStore, RequestConfig reqConf) throws WrongParam;
}
