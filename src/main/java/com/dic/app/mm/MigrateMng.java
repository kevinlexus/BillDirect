package com.dic.app.mm;

import com.ric.cmn.excp.ErrorWhileDistDeb;

public interface MigrateMng {

	void migrateDeb(String lsk, Integer period) throws ErrorWhileDistDeb;
}
