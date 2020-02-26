package com.dic.app.mm;

import com.dic.bill.Lock;
import com.dic.bill.model.scott.Tuser;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

public interface ConfigApp {

	Integer getProgress();

    String getPeriod();

    @Transactional(propagation = Propagation.REQUIRED)
	Tuser getCurUser();

    String getPeriodNext();

	String getPeriodBack();

	Date getCurDt1();

	Date getCurDt2();

	Lock getLock();

	int incNextReqNum();

	void setProgress(Integer progress);

	void incProgress();

}