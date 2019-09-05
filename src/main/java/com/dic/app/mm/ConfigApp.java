package com.dic.app.mm;

import java.util.Date;

import com.dic.bill.Lock;
import org.springframework.transaction.annotation.Transactional;

public interface ConfigApp {

	Integer getProgress();

    String getPeriod();

	String getPeriodNext();

	String getPeriodBack();

	Date getCurDt1();

	Date getCurDt2();

	Lock getLock();

	int incNextReqNum();

	void setProgress(Integer progress);

	void incProgress();

}