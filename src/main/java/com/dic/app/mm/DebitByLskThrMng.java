package com.dic.app.mm;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;

public interface DebitByLskThrMng {
	void genDebitUsl(Kart kart, CalcStore calcStore,
                     CalcStoreLocal localStore);

//	List<SumDebRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore, CalcStoreLocal localStore)
			//throws ErrorWhileChrgPen;
}
