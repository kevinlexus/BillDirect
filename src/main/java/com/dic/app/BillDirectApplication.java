package com.dic.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class BillDirectApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext app = SpringApplication.run(BillDirectApplication.class, args);

/*		ProcessMng debitMng = app.getBean(ProcessMng.class);
        debitMng.genProcessAll(null, Utl.getDateFromStr("15.04.2014"), 0);
*/    }


}
