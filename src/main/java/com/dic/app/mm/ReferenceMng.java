package com.dic.app.mm;

import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;

public interface ReferenceMng {

	UslOrg getUslOrgRedirect(UslOrg uslOrg, Kart kart, Integer tp);

}
