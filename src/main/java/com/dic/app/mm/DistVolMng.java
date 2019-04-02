package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.ric.cmn.excp.*;

public interface DistVolMng {

    void distVolByVvodTrans(RequestConfigDirect reqConf, Long vvodId)
            throws ErrorWhileDist;
}
