package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;

public interface GenChrgProcessMng {

    void genChrg(RequestConfigDirect reqConf, long klskId) throws ErrorWhileChrg;
}
