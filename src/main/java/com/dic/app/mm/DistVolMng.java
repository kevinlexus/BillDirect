package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.CommonResult;

import java.util.Date;
import java.util.concurrent.Future;

public interface DistVolMng {

    void distVolByVvod(RequestConfig reqConf);
}
