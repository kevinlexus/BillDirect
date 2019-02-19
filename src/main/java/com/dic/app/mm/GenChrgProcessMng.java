package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;

public interface GenChrgProcessMng {

    void genChrg(CalcStore calcStore, long klskId, RequestConfig reqConf) throws WrongParam, ErrorWhileChrg;
}
