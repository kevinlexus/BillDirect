package com.dic.app.mm;

import java.util.concurrent.Future;

import com.dic.bill.model.scott.SprGenItm;
import com.ric.dto.CommonResult;

public interface GenThrMng {

	Future<CommonResult> doJob(Integer var, Integer id, SprGenItm spr, double proc);

}
