package org.home.spring.app.config;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.home.spring.app.oracle.model.User;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
		  entityManagerFactoryRef = "oracleEntityManagerFactory",
		  transactionManagerRef = "oracleTransactionManager",
		  basePackages = { "org.home.spring.app.oracle.repository" }
		)
public class OracleDbConfig {

	@Bean(name = "oracleDataSource")
	@Primary
	@ConfigurationProperties(prefix = "spring.datasource")
	public DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean(name = "oracleEntityManagerFactory")
	public LocalContainerEntityManagerFactoryBean barEntityManagerFactory(EntityManagerFactoryBuilder builder,
			@Qualifier("oracleDataSource") DataSource dataSource) {
		return builder.dataSource(dataSource()).packages(User.class).persistenceUnit("user")
				.properties(jpaProperties()).build();
	}

	@Bean(name = "oracleTransactionManager")
	public PlatformTransactionManager barTransactionManager(
			@Qualifier("oracleEntityManagerFactory") EntityManagerFactory oracleEntityManagerFactory) {
		return new JpaTransactionManager(oracleEntityManagerFactory);
	}
	
	private Map<String, String> jpaProperties() {
		Map<String, String> map = new HashMap<>();
		map.put("hibernate.dialect", "org.hibernate.dialect.Oracle10gDialect");
		return map;
	}
}
