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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@ComponentScan({"com.dic.bill", "com.dic.app"}) // это нужно чтобы работали Unit-тесты! (по сути можно закомментить)
@EnableJpaRepositories(basePackages="com.dic.bill.dao")
@EnableCaching
@EnableAsync
@EntityScan(basePackages = {"com.dic.bill"})
@ImportResource("file:.\\config\\spring.xml")
public class Config  implements ApplicationContextAware, AsyncConfigurer {

	static ApplicationContext ctx = null;

/*
	@Qualifier("dataSource")
	@Autowired
	DataSource ds;
*/

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		ctx = context;
	}


	@Bean
	public CacheManager cacheManager() {
		SimpleCacheManager cacheManager = new SimpleCacheManager();
		cacheManager.setCaches(Arrays.asList(
				new ConcurrentMapCache("NaborMng.getCached"),
				new ConcurrentMapCache("PriceMng.multiplyPrice"),
				new ConcurrentMapCache("ReferenceMng.getUslOrgRedirect")));
		return cacheManager;
	}

	/**
	 * Макс количество потоков запускаемых @Async и прочие настройки
	 */
	@Override
	public Executor getAsyncExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(15);
		executor.setMaxPoolSize(20);
		executor.setQueueCapacity(100);
		executor.setThreadNamePrefix("BillDirectExecutor-");

		// обработчик, если места нет в пуле для потока - с просьбой подождать места
		// https://stackoverflow.com/questions/49290054/taskrejectedexception-in-threadpooltaskexecutor
		executor.setRejectedExecutionHandler(new RejectedExecutionHandlerImpl());
		executor.initialize();
		return executor;
	}

	public static ApplicationContext getContext(){
	      return ctx;
	}

}
