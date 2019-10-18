package com.dic.app.mm.impl;

import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;

import java.util.List;

/**
 * подсчет
 */
public class Cnt {
	public long cntSal = 0L;
	public long cntDeb = 0L;
	public List<SumDebUslMgRec> lstSalNd;
	public List<SumDebMgRec> lstDebNd;
}
