package com.dic.app.mm;

import java.util.Date;
import java.util.List;

import com.dic.bill.model.scott.SprGenItm;

public interface ExecMng {

	void updateSprGenItem(List<SprGenItm> lst);
	Integer execProc(Integer var, Integer id, Integer sel);
	void clearError(SprGenItm menuGenItg);
	void setGenDate();
	void stateBase(int state);
	void setMenuElemPercent(SprGenItm spr, double proc);
	void clearPercent();
	void setMenuElemState(SprGenItm spr, String state);
	void setMenuElemDt1(SprGenItm spr, Date dt1);
	void setMenuElemDt2(SprGenItm spr, Date dt2);

}
