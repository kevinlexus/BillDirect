package com.dic.app.mm.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@AllArgsConstructor
@Setter
@Getter
public class DebitRegistryEls {
   String els;
   String houseGUID;
   String kw;
   String fio;
   String adr;
   String ukName;
   String period;
   BigDecimal deb;
}
