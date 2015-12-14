package com.jakartawebs.learn.integration;


import java.beans.PropertyEditor;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.propertyeditors.CustomDateEditor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration of import user job.
 * 
 * @author zakyalvan
 */
@Configuration
@EnableBatchProcessing
public class BatchIntegrationConfiguration {
	@Value("${user.home}")
	private String homeDirectory;
	
	/**
	 * Transforming a {@link File} message into {@link JobLaunchRequest} message.
	 * 
	 * @author zakyalvan
	 */
	@Component("transformer")
	public static class FileMessageToJobLaunchRequestTransformer {
		private static final Logger LOGGER = LoggerFactory.getLogger(FileMessageToJobLaunchRequestTransformer.class);
		
		private Job importJob;
		private String fileParameterName;
		
		@Autowired
		public FileMessageToJobLaunchRequestTransformer(@Qualifier("importJob") Job importJob, @Value("input.file.name") String fileParameterName) {
			Assert.notNull(importJob);
			Assert.notNull(fileParameterName);
			
			this.importJob = importJob;
			this.fileParameterName = fileParameterName;
		}
		
		@Transformer
		public JobLaunchRequest createJobLaunchRequest(Message<File> fileMessage) {
			LOGGER.info("Create launch job request object from file message");
			JobParametersBuilder parametersBuilder = new JobParametersBuilder();
			parametersBuilder.addString(fileParameterName, fileMessage.getPayload().getAbsolutePath());		
			return new JobLaunchRequest(importJob, parametersBuilder.toJobParameters());
		}
	}
	
	/**
	 * FIXME
	 * 
	 * Modify so that status evaluation done here.
	 * Send email when not completed.
	 * 
	 * @return
	 */
	@Bean
	public IntegrationFlow statusEvaluationFlow() {
		return flow -> flow.handle(message -> System.out.println("6666666666666666666"));
	}
	
	@Bean
	public JobLaunchingGateway launchJobGateway(JobLauncher jobLauncher) {
		JobLaunchingGateway launchJobGateway = new JobLaunchingGateway(jobLauncher);
		launchJobGateway.setOutputChannelName("statusEvaluationFlow.input");
		return launchJobGateway;
	}
	
	@Bean
	public IntegrationFlow fileFlow(FileMessageToJobLaunchRequestTransformer transformer, JobLaunchingGateway launchJobGateway) throws IOException {
		return IntegrationFlows.from(Files.inboundAdapter(new File(homeDirectory + File.separator + "FTP" + File.separator + "batch" + File.separator + "integration")).autoCreateDirectory(true), spec -> spec.poller(Pollers.fixedDelay(5000)))
				.transform(transformer)
				.handle(launchJobGateway)
				.get();
	}
	
	/**
	 * Line mapper for flat file item reader used on step.
	 * 
	 * @return
	 */
	@Bean
	public LineMapper<Person> personLineMapper() {
		return new DefaultLineMapper<Person>() {{
			setLineTokenizer(new FixedLengthTokenizer() {{
				setNames(new String[] {"firstName", "lastName", "emailAddress", "dateOfBirth"});
				
				Range firstNameColumn = new Range(1, 20);
				Range lastNameColumn = new Range(20, 40);
				Range emailAddressColumn = new Range(40, 70);
				Range dateOfBirthColumn = new Range(70, 80);
				
				setColumns(new Range[] {firstNameColumn, lastNameColumn, emailAddressColumn, dateOfBirthColumn});
			}});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
				setTargetType(Person.class);
				
				/**
				 * Enable binding date type with custom format or pattern.
				 * 
				 * @see http://www.mkyong.com/spring-batch/how-to-convert-date-in-beanwrapperfieldsetmapper/.
				 */
				Map<Object, PropertyEditor> customEditors = new HashMap<>();
				customEditors.put(Date.class, new CustomDateEditor(new SimpleDateFormat("dd-MM-yyyy"), false));
				setCustomEditors(customEditors);
			}});
		}};
	}
	
	/**
	 * Item reader bean component.
	 * 
	 * @author zakyalvan
	 */
	@Component("reader") @StepScope
	public static class UserFlatFileItemReader extends FlatFileItemReader<Person> {
		@Value("file://#{jobParameters['input.file.name']}")
		private Resource fixedLengthUserDataFileResource;

		@Autowired
		public UserFlatFileItemReader(@Qualifier("personLineMapper") LineMapper<Person> personLineMapper) {
			super();
			Assert.notNull(personLineMapper);
			this.setLineMapper(personLineMapper);
		}
		
		@Override
		public void afterPropertiesSet() throws Exception {
			this.setResource(fixedLengthUserDataFileResource);
			super.afterPropertiesSet();
		}
	}
	
	@Bean(name="processor") @StepScope
	public ItemProcessor<Person, Person> processor() {
		return (@Validated Person person) -> {
			Person processedPerson = new Person(person.getFirstName().toUpperCase(), person.getLastName().toUpperCase(), person.getEmailAddress(), person.getDateOfBirth());
			System.out.println("========================= Processed person : " + processedPerson);
			return processedPerson;
		};
	}
	
	@Bean(name="writer") @StepScope
	public ItemWriter<Person> writer(DataSource dataSource) {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(dataSource);
		writer.setSql("INSERT INTO integration_people (first_name, last_name, email_address, date_of_birth) VALUES (:firstName, :lastName, :emailAddress, :dateOfBirth)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		return writer;
	}
	
	@Bean
	public Step importStep(StepBuilderFactory steps, ItemReader<Person> reader, ItemProcessor<Person, Person> processor, ItemWriter<Person> writer) {
		return steps.get("importStep")
				.<Person, Person> chunk(10)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}
	
	/**
	 * {@link JobExecutionListener} for verifying loaded data.
	 * 
	 * @author zakyalvan
	 */
	@Component("importListener")
	public static class ImportUserExecutionListener extends JobExecutionListenerSupport {
		private static final Logger LOGGER = LoggerFactory.getLogger(ImportUserExecutionListener.class);
		
		private final JdbcTemplate jdbcTemplate;

		@Autowired
		public ImportUserExecutionListener(JdbcTemplate jdbcTemplate) {
			Assert.notNull(jdbcTemplate);
			this.jdbcTemplate = jdbcTemplate;
		}

		@Override
		public void afterJob(JobExecution jobExecution) {
			LOGGER.info("Import user job execution completed, time to verify if job completed successfully success");
			if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
				List<Person> results = jdbcTemplate.query("SELECT first_name, last_name, email_address, date_of_birth FROM integration_people", new RowMapper<Person>() {
					@Override
					public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
						return new Person(rs.getString(1), rs.getString(2), rs.getString(3), rs.getDate(4));
					}
				});
				
				results.forEach(person -> LOGGER.info("Found {}", person));
			}
			else {
				LOGGER.error("Import user job execution isn't completed successfully (Status other than COMPLETED)");
			}
		}
	}
	
	@Bean
	public Job importJob(JobBuilderFactory jobs, @Qualifier("importStep") Step importStep, @Qualifier("importListener") JobExecutionListener executionListener) {
		return jobs.get("importJob")
				.incrementer(new RunIdIncrementer())
				.listener(executionListener)
				.flow(importStep)
				.end()
				.build();
	}
	
	@Bean
	public JdbcTemplate jdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}
}
