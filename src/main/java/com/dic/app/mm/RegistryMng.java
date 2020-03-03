package com.dic.app.mm;

import java.io.FileNotFoundException;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(String cityName, String reu, String lskTp, String fileName, String codePage) throws FileNotFoundException;
    int loadFileMeterVal(String fileName, String codePage, boolean isSetPreviosVal) throws FileNotFoundException;
    void loadApprovedKartExt();
}
