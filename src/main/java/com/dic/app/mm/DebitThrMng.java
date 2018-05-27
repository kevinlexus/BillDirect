package com.dic.app.mm;

import java.util.List;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumPenRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface DebitThrMng {

	List<SumPenRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore, CalcStoreLocal localStore)
			throws ErrorWhileChrgPen;
}
