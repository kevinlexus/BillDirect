package com.dic.app; 

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class BillDirectApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext app = SpringApplication.run(BillDirectApplication.class, args);
    }

}
