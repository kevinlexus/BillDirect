package com.dic.app.mm;

import java.util.Date;
import java.util.concurrent.Future;

import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.SessionDirect;
import com.ric.cmn.CommonResult;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface DebitMng {

	void genDebitAll(String lsk, Date genDt, Integer debugLvl, SessionDirect sessionDirect) throws ErrorWhileChrgPen;
	Future<CommonResult> genDebit(String lsk, CalcStore calcStore, Integer tp, SessionDirect sessionDirect);
}
