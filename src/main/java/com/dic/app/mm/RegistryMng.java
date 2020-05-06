package com.dic.app.mm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(String cityName, String reu, String uslId, String lskTp, String fileName) throws FileNotFoundException;

    int unloadPaymentFileKartExt(String filePath, String codeUk, Date genDt1, Date genDt2) throws IOException;

    int loadFileMeterVal(String fileName, String codePage, boolean isSetPreviosVal) throws FileNotFoundException;
    int unloadFileMeterVal(String fileName, String codePage, String strUk) throws IOException;

    void loadApprovedKartExt();
}
