package com.dic.app.mm;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;

public interface GenChrgProcessMng {

    void genChrg(CalcStore calcStore, Kart kart);
    void countPers();
}
