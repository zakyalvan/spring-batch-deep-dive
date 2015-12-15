package com.jakartawebs.learn.integration;


import java.beans.PropertyEditor;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.validation.groups.Default;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.ItemListenerSupport;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.listener.SkipListenerSupport;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.propertyeditors.CustomDateEditor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.mail.MailHeaders;
import org.springframework.integration.mail.MailSendingMessageHandler;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.mail.MailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * Configuration of import user job.
 * 
 * @author zakyalvan
 */
@Configuration
@EnableBatchProcessing
@IntegrationComponentScan
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
		return flow -> flow
				.<JobExecution, Boolean>route(jobExec -> jobExec.getStatus() == BatchStatus.COMPLETED, mapping -> mapping.subFlowMapping("true", subFlow -> subFlow.channel(MessageHeaders.ERROR_CHANNEL)));
	}
	
	@Autowired
	private JavaMailSender mailSender;
	
	@Bean
	public MailSendingMessageHandler mailSendingMessageHandler() {
		return new MailSendingMessageHandler(mailSender);
	}
	@Bean
	public IntegrationFlow mailSendingFlow() {
		return flow -> flow.handle(mailSendingMessageHandler());
	}
	
	@MessagingGateway
	public static interface MailSendingGateway {
		@Gateway(requestChannel="mailSendingFlow.input")
		void sendAsAsMailMessage(Object object);
	}
	
	
	/**
	 * Please note, error channel will only catch error occured on asynch flow (Processed outside caller thread).
	 */
	@Autowired @Qualifier(MessageHeaders.ERROR_CHANNEL)
	private MessageChannel errorChannel;
	
	@Bean
	public IntegrationFlow errorNotificationFlow() {
		return IntegrationFlows.from(errorChannel)
				.route(object -> MailMessage.class.isAssignableFrom(object.getClass()), mapping -> mapping.subFlowMapping("false", subFlow -> subFlow.enrichHeaders(spec -> spec.header(MailHeaders.TO, "zaky@jakartawebs.com").header(MailHeaders.FROM, "zaky@jakartawebs.com").header(MailHeaders.SUBJECT, "Error On Integration Flow"))))
				.handle(mailSendingFlow())
				.get();
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
				 * Enable binding {@link java.util.Date} type with custom date format or pattern.
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
	
	@Autowired
	public javax.validation.Validator validator() {
		ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
		return validatorFactory.getValidator();
	}
	
	@Bean(name="validateProcess") @StepScope
	public ItemProcessor<Person, Person> validateProcess() {
		ValidatingItemProcessor<Person> validateProcess = new ValidatingItemProcessor<>();
		
		Validator<Person> personValidator = new Validator<Person>() {
			@Override
			public void validate(Person value) throws ValidationException {
				Set<ConstraintViolation<Person>> violations = validator().validate(value, Default.class);
				if(violations.size() > 0) {
					throw new ValidationException("Bean validation failed", new ConstraintViolationException(violations));
				}
			}
		};
		
		validateProcess.setValidator(personValidator);
		return validateProcess;
	}
	@Bean(name="uppercaseProcess") @StepScope
	public ItemProcessor<Person, Person> uppercaseProcess() {
		return person -> new Person(person.getFirstName().toUpperCase(), person.getLastName().toUpperCase(), person.getEmailAddress(), person.getDateOfBirth());
	}
	@Bean(name="processor") @StepScope
	public ItemProcessor<Person, Person> processor() {
		CompositeItemProcessor<Person, Person> processor = new CompositeItemProcessor<>();
		processor.setDelegates(Arrays.asList(validateProcess(), uppercaseProcess()));
		return processor;
	}
	
	@Bean(name="writer") @StepScope
	public ItemWriter<Person> writer(DataSource dataSource) {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(dataSource);
		writer.setSql("INSERT INTO integration_people (first_name, last_name, email_address, date_of_birth) VALUES (:firstName, :lastName, :emailAddress, :dateOfBirth)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		return writer;
	}
	
	@Bean @StepScope
	public ItemReadListener<Person> readListener() {
		return new ItemListenerSupport<Person, Person>() {
			@Override
			public void onReadError(Exception ex) {
				System.err.println("@@@@@@@@@@@@@@@@@@ Read error with exception : " + ex.toString());
			}
		};
	}
	
	@Bean @StepScope
	public SkipListener<Person, Person> skipListener() {
		return new SkipListenerSupport<Person, Person>() {
			@Override
			public void onSkipInRead(Throwable t) {
				System.err.println("$$$$$$$$$$$$$ Exception type : " + t.getClass().getName());
				if(FlatFileParseException.class.isAssignableFrom(t.getClass())) {
					FlatFileParseException parseException = (FlatFileParseException) t;
					System.err.println("$$$$$$$$$$$$$$$$$$ Skipped invalid line " + parseException.getInput() + ", found on line number " + parseException.getLineNumber());
				}
			}

			@Override
			public void onSkipInProcess(Person item, Throwable t) {
				System.err.println("Failed on processing of item : " + item + ", with exception message : " + t.getMessage());
			}
		};
	}
	
	@Bean
	public Step importStep(StepBuilderFactory steps, ItemReader<Person> reader, ItemProcessor<Person, Person> processor, ItemWriter<Person> writer) {
		return steps.get("importStep")
				.<Person, Person> chunk(10).reader(reader).processor(processor).writer(writer)
				.faultTolerant().skip(FlatFileParseException.class).skip(ValidationException.class).skipLimit(10)
				/**
				 * If read listener set first (before skip listener) then skip listener wont be executed.
				 * Please note, item read listener methods called immediately after item reader executions,
				 * in other hand, skip listener called on after chunk transaction failed.
				 */
				.listener(skipListener())
				.listener(readListener())
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
				List<Person> results = jdbcTemplate.query("SELECT person_id, first_name, last_name, email_address, date_of_birth FROM integration_people", new RowMapper<Person>() {
					@Override
					public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
						return new Person(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getDate(5));
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
