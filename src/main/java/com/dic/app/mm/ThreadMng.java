package com.dic.app.mm;

import java.util.List;

import com.dic.bill.dto.CalcStore;

public interface ThreadMng {

	void invokeThreads(CalcStore calcStore, int cntThreads, List<String> lstItem);

}
