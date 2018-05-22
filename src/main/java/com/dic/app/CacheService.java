package com.dic.app;


import java.util.concurrent.TimeUnit;

import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

import org.springframework.boot.autoconfigure.cache.JCacheManagerCustomizer;
import org.springframework.stereotype.Component;

/**
 * Набор кэшей для Ehcache
 * @author lev
 * @version 1.00
 */
public class CacheService {


	  @Component
	  public static class CachingSetup implements JCacheManagerCustomizer
	  {

		@Override
	    public void customize(CacheManager cacheManager)
	    {
	      cacheManager.createCache("default", new MutableConfiguration<>()
	        .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 60)))
	        .setStoreByValue(false)
	        .setStatisticsEnabled(false));
	    }

	  }

}
