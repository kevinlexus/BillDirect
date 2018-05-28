package com.dic.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.dic.app.mm.DebitMng;
import com.ric.cmn.Utl;


@SpringBootApplication
public class BillDirectApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext app = SpringApplication.run(BillDirectApplication.class, args);

		DebitMng debitMng = app.getBean(DebitMng.class);

        debitMng.genDebitAll(null, Utl.getDateFromStr("15.04.2014"), 0);
    }


}
