package com.dic.app.mm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

@Controller
@Slf4j
public class WebController {

	@Autowired
	private DebitMng debitMng;

    @GetMapping("/genDebitPen")
    public String genDebitPen() {

    	debitMng.genDebitAll(null, Utl.getDateFromStr("15.04.2014"), 0);

        return "OK";
    }

}