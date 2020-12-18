package com.dic.app.mm;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface DebitByLskThrMng {
	void genDeb(Kart kart, CalcStore calcStore,
				CalcStoreLocal localStore) throws ErrorWhileChrgPen;

//	List<SumDebRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore, CalcStoreLocal localStore)
			//throws ErrorWhileChrgPen;
}
