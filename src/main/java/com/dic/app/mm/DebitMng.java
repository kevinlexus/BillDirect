package com.dic.app.mm;

import java.util.Date;
import java.util.concurrent.Future;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.CommonResult;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface DebitMng {

	Future<CommonResult> genDebit(String lsk, CalcStore calcStore, RequestConfig reqConf);
	void genDebitAll(String lskFrom, String lskTo, Date genDt, Integer debugLvl, RequestConfig reqConf)
			throws ErrorWhileChrgPen;
}
