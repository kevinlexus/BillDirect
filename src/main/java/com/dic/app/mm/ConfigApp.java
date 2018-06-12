package com.dic.app.mm;

import java.util.Date;

import com.dic.bill.Lock;

public interface ConfigApp {

	Integer getProgress();

	String getPeriod();

	String getPeriodNext();

	String getPeriodBack();

	Date getCurDt1();

	Date getCurDt2();

	Lock getLock();

	boolean aquireLock(int rqn, String lsk);

	int incNextReqNum();

	String getStateGen();

	void setStateGen(String stateGen);

	void setProgress(Integer progress);

}