package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.excp.*;
import com.ric.dto.CommonResult;

import java.util.Date;
import java.util.concurrent.Future;

public interface DistVolMng {

    void distVolByVvod(RequestConfig reqConf, CalcStore calcStore) throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist;
}
