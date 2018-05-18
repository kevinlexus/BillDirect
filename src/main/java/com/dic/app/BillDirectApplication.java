package com.dic.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;


@SpringBootApplication
public class BillDirectApplication {

	private static ApplicationContext applicationContext = null;

	public static void main(String[] args) {
		SpringApplication.run(BillDirectApplication.class, args);
    }
}
