package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.*;

public interface DistVolMng {

    void distVolByVvod(RequestConfig reqConf, CalcStore calcStore, Integer vvodId)
            throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist;
}
