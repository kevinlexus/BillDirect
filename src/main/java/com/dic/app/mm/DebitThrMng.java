package com.dic.app.mm;

import java.util.List;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface DebitThrMng {
	void genDebitUsl(Kart kart, CalcStore calcStore,
					 CalcStoreLocal localStore);

//	List<SumDebRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore, CalcStoreLocal localStore)
			//throws ErrorWhileChrgPen;
}
