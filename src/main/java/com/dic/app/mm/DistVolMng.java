package com.dic.app.mm;

import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.*;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface DistVolMng {

    void distVolByVvodTrans(RequestConfig reqConf, CalcStore calcStore, Integer vvodId)
            throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist, ErrorWhileGen;

    @Transactional(
            propagation = Propagation.MANDATORY, // та же транзакция
            rollbackFor = Exception.class)
    void distVolByVvodSameTrans(RequestConfig reqConf, CalcStore calcStore, Integer vvodId)
            throws ErrorWhileChrgPen, WrongParam, WrongGetMethod, ErrorWhileDist, ErrorWhileGen;
}
