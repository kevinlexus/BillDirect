package com.dic.app.mm;

import java.util.concurrent.Future;

import com.ric.cmn.excp.ErrorWhileDistDeb;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.dto.CommonResult;

public interface MigrateMng {

	void migrateAll(String lskFrom, String lskTo, Integer dbgLvl) throws ErrorWhileGen;
	Future<CommonResult> migrateDeb(String lsk, Integer periodBack, Integer period, Integer dbgLvl);

}
