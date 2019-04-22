package com.dic.app.mm;

import com.ric.cmn.excp.ErrorWhileDistPay;

public interface DistPayMng {

    void distKwtpMg(int kwtpMgId) throws ErrorWhileDistPay;

    void distSalCorrOperation();
}
