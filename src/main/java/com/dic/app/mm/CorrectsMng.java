package com.dic.app.mm;

import com.dic.bill.model.scott.ChangeDoc;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Tuser;
import com.ric.cmn.excp.WrongParam;

import java.math.BigDecimal;
import java.util.Date;

public interface CorrectsMng {
    void corrPayByCreditSal(int var, Date dt, String uk) throws WrongParam;

    void saveCorrects(String period, Tuser user, Date dt, ChangeDoc changeDoc, Kart kart,
                      String uslId, Integer orgId, BigDecimal summa);
}
