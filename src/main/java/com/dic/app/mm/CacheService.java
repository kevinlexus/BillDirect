package com.dic.app.mm;


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
 *
 */
public class CacheService {


	  @Component
	  public static class CachingSetup implements JCacheManagerCustomizer
	  {

		@Override
	    public void customize(CacheManager cacheManager)
	    {
	      cacheManager.createCache("ReferenceMng.getUslOrgRedirect", new MutableConfiguration<>()
		  	        .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 600)))
		  	        .setStoreByValue(true)
		  	        .setStatisticsEnabled(false));
	      cacheManager.createCache("UtlMngImpl.between2_str", new MutableConfiguration<>()
		  	        .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 60000)))
		  	        .setStoreByValue(false)
		  	        .setStatisticsEnabled(false));
	    }




	  }

}
