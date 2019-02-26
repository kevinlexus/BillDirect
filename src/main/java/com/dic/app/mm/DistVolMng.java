package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.*;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface DistVolMng {

    void distVolByVvodTrans(RequestConfigDirect reqConf, Integer vvodId)
            throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist, ErrorWhileGen;

    void distVolByVvodSameTrans(RequestConfigDirect reqConf, Integer vvodId)
            throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist, ErrorWhileGen;
}
