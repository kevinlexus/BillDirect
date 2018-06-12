package com.dic.app.mm;

import java.util.List;

import com.dic.bill.model.scott.SprGenItm;

public interface GenMng {

	void updateSprGenItem(List<SprGenItm> lst);

	Integer execProc(Integer var, Integer id, Integer sel);

	void clearError(SprGenItm menuGenItg);

	void setGenDate();

	void stateBase(int state);
}
