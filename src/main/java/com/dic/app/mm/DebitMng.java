package com.dic.app.mm;

import java.util.Date;
import java.util.concurrent.Future;

import com.dic.bill.dto.CalcStore;
import com.ric.cmn.CommonResult;

public interface DebitMng {

	void genDebitAll(String lsk, Date genDt, Integer debugLvl);
	Future<CommonResult> genDebit(String lsk, CalcStore calcStore);
}
