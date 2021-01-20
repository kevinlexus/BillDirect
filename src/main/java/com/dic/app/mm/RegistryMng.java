package com.dic.app.mm;

import com.dic.bill.model.scott.Org;
import com.ric.cmn.excp.ErrorWhileLoad;
import com.ric.cmn.excp.WrongParam;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(Org org, String fileName) throws FileNotFoundException, WrongParam, ErrorWhileLoad;

    int unloadPaymentFileKartExt(String filePath, String codeUk, Date genDt1, Date genDt2) throws IOException;

    int loadFileMeterVal(String fileName, String codePage, boolean isSetPreviosVal) throws FileNotFoundException;
    int unloadFileMeterVal(String fileName, String codePage, String strUk) throws IOException;

    void loadApprovedKartExt(Org org) throws WrongParam;
}
