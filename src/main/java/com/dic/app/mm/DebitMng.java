package com.dic.app.mm;

import java.util.Date;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Kart;

public interface DebitMng {

	public void genDebitAll(String lsk, Date genDt, Integer debugLvl);
	public void genDebit(Kart kart, CalcStore calcStore);

}
