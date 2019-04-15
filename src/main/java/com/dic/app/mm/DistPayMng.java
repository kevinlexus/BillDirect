package com.dic.app.mm;

import com.dic.bill.model.scott.KwtpMg;
import com.ric.cmn.excp.ErrorWhileDistPay;

public interface DistPayMng {

    void distKwtpMg(KwtpMg kwtpMg) throws ErrorWhileDistPay;

    void distSalCorrOperation();
}
