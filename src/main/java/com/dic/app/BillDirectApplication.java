package com.dic.app;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.RequestConfig;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class BillDirectApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext app = SpringApplication.run(BillDirectApplication.class, args);
    }


}
