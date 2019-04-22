package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.model.scott.*;
import com.ric.cmn.excp.ErrorWhileChrg;

import java.util.Date;

public interface GenChrgProcessMng {

    String genChrg(int tp, int debugLvl, Date genDt, House house, Vvod vvod, Ko ko, Org uk, Usl usl);
    void genChrg(RequestConfigDirect reqConf, long klskId) throws ErrorWhileChrg;
}
