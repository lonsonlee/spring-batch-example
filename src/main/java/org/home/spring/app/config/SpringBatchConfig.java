package org.home.spring.app.config;

import javax.sql.DataSource;

import org.home.spring.app.oracle.model.User;
import org.home.spring.batch.DBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.DatabaseType;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	
    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory,
                   StepBuilderFactory stepBuilderFactory,
                   ItemReader<User> itemReader,
                   ItemProcessor<User, User> itemProcessor,
                   ItemWriter<User> itemWriter
    ) {

        Step step = stepBuilderFactory.get("ETL-file-load")
                .<User, User>chunk(100)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();


        return jobBuilderFactory.get("ETL-Load")
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public FlatFileItemReader<User> itemReader(@Value("${input}") Resource resource) {

        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(resource);
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }
    
    @Bean
    public ItemWriter<User> itemWriter() {
    		return new DBWriter();
    }
    
//    @Bean
//    public LocalContainerEntityManagerFactoryBean userEntityManagerFactory() {
//    	LocalContainerEntityManagerFactoryBean factoryBean = new 
//    			LocalContainerEntityManagerFactoryBean();
//    			    factoryBean.setDataSource(orderDataSource());
//    			    factoryBean.setPackagesToScan("org.home.batch");
//    			    factoryBean.setJpaVendorAdapter(jpaVendorAdapter());
//    			    factoryBean.setJpaProperties(jpaProperties());
//    			    return factoryBean;
    			    
//    		return builder
//    			.dataSource(orderDataSource())
//    			.packages(User.class)
//    			.persistenceUnit("orders")
//    			.build();
//    }
    
//    <bean id="dataSource" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">
//    <property name="driverClassName" value="oracle.jdbc.driver.OracleDriver"/>
//    <property name="url" value="jdbc:oracle:thin:@oracle.devcake.co.uk:1521:INTL"/>
//    <property name="username" value="sa"/>
//    <property name="password" value=""/>
//</bean>
    

//<bean id="transactionManager" class="org.springframework.jdbc.datasource.DataSourceTransactionManager" lazy-init="true">
//	<property name="dataSource" ref="dataSource" />
//</bean>
    
//    <bean id="jobRepository"
//    	      class="org.springframework.batch.core.repository.support.JobRepositoryFactoryBean">
//    	        <property name="dataSource" ref="dataSource" />
//    	        <property name="transactionManager" ref="transactionManager" />
//    	        <property name="databaseType" value="sqlite" />
//    	    </bean>
    
//    private DataSource orderDataSource() {
//    		BasicDataSource dataSource=new BasicDataSource();
//    		dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
//    		dataSource.setUrl("jdbc:oracle:thin:@ech-10-157-151-74.mastercard.int:1527:DEVCLOUD");
//    		dataSource.setUsername("DEVADMIN");
//    		dataSource.setPassword("amit@1234");
//		return dataSource;
//	}

	private JobRepository jobRepository(JobRepositoryFactoryBean jobRepositoryFactoryBean) throws Exception {
    		jobRepositoryFactoryBean.setDatabaseType(DatabaseType.H2.getProductName());
    		jobRepositoryFactoryBean.setDataSource(secondaryDataSource());
    		jobRepositoryFactoryBean.setTransactionManager(transactionManager());
		return jobRepositoryFactoryBean.getObject();
    }
    
    @Bean
    public JobLauncher jobLauncher() throws Exception {
    		final JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
    		SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
    		simpleJobLauncher.setJobRepository(jobRepository(jobRepositoryFactoryBean));
    		return simpleJobLauncher;
    }
    
//    @Bean
//    public DataSource h2DataSource() {
//        DriverManagerDataSource dataSource = new DriverManagerDataSource();
//        dataSource.setDriverClassName("org.h2.Driver");
//        dataSource.setUrl("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1");
//        dataSource.setUsername("sa");
//        dataSource.setPassword("sa");
//        return dataSource;
//    }
    
    @Bean
    public ResourcelessTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }
    
//    @Bean(destroyMethod = "shutdown")
//    public EmbeddedDatabase dataSourceH2() {
//        return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
//                .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
//                .addScript("classpath:org/springframework/batch/core/schema-h2.sql").build();
//    }
    
    @Bean(name="h2datastore")
	@ConfigurationProperties(prefix="spring.h2datasource")
	public DataSource secondaryDataSource() {
	    return DataSourceBuilder.create().build();
	}
    
    @Bean
    public LineMapper<User> lineMapper() {

        DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "name", "dept", "salary");

        BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(User.class);

        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }
    
    @Bean
    BatchConfigurer configurer(@Qualifier("h2datastore") DataSource dataSource){
        return new DefaultBatchConfigurer(dataSource);
    }
    
    @Value("classpath:/org/springframework/batch/core/schema-h2.sql")
    private Resource schemaScript;

    @Value("classpath:/org/springframework/batch/core/schema-drop-h2.sql")
    private Resource dropScript;

    @Bean
    public DataSourceInitializer dataSourceInitializer(@Qualifier("h2datastore") DataSource dataSource) {
        final DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulatorInit());
        initializer.setDatabaseCleaner(databasePopulatorCleaner());
        return initializer;
    }

    private DatabasePopulator databasePopulatorInit() {
        final ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(dropScript);
        populator.addScript(schemaScript);
        return populator;
    }
    
    private DatabasePopulator databasePopulatorCleaner() {
        final ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(dropScript);
        return populator;
    }

}
