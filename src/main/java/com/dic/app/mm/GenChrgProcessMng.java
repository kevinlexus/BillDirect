package com.dic.app.mm;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.WrongParam;

public interface GenChrgProcessMng {

    void genChrg(CalcStore calcStore, Kart kart) throws WrongParam;
}
