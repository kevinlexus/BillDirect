package com.dic.app.mm;

import java.util.Date;
import java.util.concurrent.Future;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.CommonResult;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface ProcessMng {

    void genProcessAll(RequestConfig reqConf, CalcStore calcStore) throws ErrorWhileChrgPen;

    CalcStore buildCalcStore(Date genDt, Integer debugLvl);

    Future<CommonResult> genProcess(int klskId, CalcStore calcStore, RequestConfig reqConf) throws WrongParam, ErrorWhileChrg;
}
