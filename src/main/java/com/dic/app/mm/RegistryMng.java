package com.dic.app.mm;

import java.io.FileNotFoundException;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(String cityName, String reu, String lskTp, String fileName, String codePage) throws FileNotFoundException;

    void loadApprovedKartExt();
}
