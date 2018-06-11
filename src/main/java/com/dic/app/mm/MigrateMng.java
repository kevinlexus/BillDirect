package com.dic.app.mm;

import java.util.concurrent.Future;

import com.ric.cmn.CommonResult;
import com.ric.cmn.excp.ErrorWhileDistDeb;

public interface MigrateMng {

	void migrateAll(String lskFrom, String lskTo) throws ErrorWhileDistDeb;
	Future<CommonResult> migrateDeb(String lsk, Integer periodBack, Integer period);

}
