package com.dic.app.mm;

import com.dic.bill.dto.KwtpMgRec;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface DistPayQueueMng {
    void queueKwtpMg(KwtpMgRec kwtpMgRec);
}
