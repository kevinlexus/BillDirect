package com.dic.app.mm;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(String cityName, String reu, String lskTp, String fileName, String codePage) throws FileNotFoundException;
    int loadFileMeterVal(String fileName, String codePage, boolean isSetPreviosVal) throws FileNotFoundException;
    int unloadFileMeterVal(String fileName, String codePage, String strUk) throws IOException;

    void loadApprovedKartExt();
}
