package com.dic.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.concurrent.ConcurrentMapCacheFactoryBean;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.*;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@ComponentScan({"com.dic.bill", "com.dic.app"}) // это нужно чтобы работали Unit-тесты! (по сути можно закомментить)
@EnableJpaRepositories(basePackages="com.dic.bill.dao")
@EnableTransactionManagement
@EnableCaching
@EnableAsync
@EntityScan(basePackages = {"com.dic.bill"})
@ImportResource("file:.\\config\\spring.xml")
public class Config  implements ApplicationContextAware, AsyncConfigurer {

	private static ApplicationContext ctx = null;

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		ctx = context;
	}

	@Bean
	public CacheManager cacheManager() {
		SimpleCacheManager cacheManager = new SimpleCacheManager();
		cacheManager.setCaches(Arrays.asList(
				new ConcurrentMapCache("NaborMng.getCached"),
				new ConcurrentMapCache("KartMng.getKartMainLsk"),
				new ConcurrentMapCache("PriceMng.multiplyPrice"),
				new ConcurrentMapCache("ReferenceMng.getUslOrgRedirect")));
		return cacheManager;
	}

	public static ApplicationContext getContext(){
	      return ctx;
	}

	@Bean
	public Executor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(200);
		executor.setMaxPoolSize(500);
		executor.setQueueCapacity(5000);
		executor.setThreadNamePrefix("BillDirect-");
		executor.setRejectedExecutionHandler(new RejectedExecutionHandlerImpl());
		executor.initialize();
		return executor;
	}
}
