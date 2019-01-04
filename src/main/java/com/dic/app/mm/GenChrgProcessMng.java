package com.dic.app.mm;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;

public interface GenChrgProcessMng {

    void genChrg(CalcStore calcStore, Integer klskId) throws WrongParam, ErrorWhileChrg;
}
