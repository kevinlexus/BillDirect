package com.dic.app.mm;

import com.dic.app.mm.impl.GenPenMngImpl;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Pen;

import java.math.BigDecimal;
import java.util.Date;

public interface GenPenMng {
    GenPenMngImpl.PenDTO getPen(CalcStore calcStore, BigDecimal summa, Integer mg, Kart kart, Date curDt);
}
