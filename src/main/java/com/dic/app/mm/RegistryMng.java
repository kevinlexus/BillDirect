package com.dic.app.mm;

import org.springframework.transaction.annotation.Transactional;

import java.io.FileNotFoundException;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(String cityName, String reu, String lskTp, String fileName, String codePage) throws FileNotFoundException;

    @Transactional
    int loadFileMeterVal(String fileName, String codePage) throws FileNotFoundException;

    void loadApprovedKartExt();
}
