package com.dic.app.mm;

import com.ric.cmn.excp.ErrorWhileDistPay;

public interface DistPayMng {

    void distKwtpMg(int kwtpMgId,
                    String lsk,
                    String strSumma,
                    String strPenya,
                    String strDebt,
                    String dopl,
                    int nink,
                    String nkom,
                    String oper,
                    String dtekStr,
                    String datInkStr, boolean isTest) throws ErrorWhileDistPay;

    void distSalCorrOperation();
}
