package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.dto.CommonResult;

import java.util.Date;
import java.util.concurrent.Future;

public interface GenPenProcessMng {

    void genDebitPen(CalcStore calcStore, Kart kart);
}