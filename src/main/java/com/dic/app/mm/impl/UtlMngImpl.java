package com.dic.app.mm.impl;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.dic.app.mm.UtlMng;
import com.ric.cmn.Utl;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис-прокси для UTL - не взлетело. Само кэширование по времени заняло больше чем вычисление функций
 * Удалить позже!
 * @author Lev
 *
 */
@Slf4j
@Service
public class UtlMngImpl implements UtlMng {



	@Override
	@Cacheable(cacheNames="UtlMngImpl.between2_str", key="{#checkReu, #reu4From, #reuTo}" )
	public boolean between2(String checkReu, String reuFrom, String reuTo) {
		log.info("CACHE {}, {}, {}", checkReu, reuFrom, reuTo);
		return Utl.between2(checkReu, reuFrom, reuTo);

	}



}
