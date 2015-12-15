package com.jakartawebs.learn.integration;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.mail.MailSendingMessageHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.mail.MailMessage;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Configuration
public class LaunchImportUserJobFlowConfiguration {
	@Value("file://${user.home}/FTP/batch/integration")
	private Resource watchedDirectoryResource;
	
	/**
	 * Transforming a {@link Message<File>} type into {@link JobLaunchRequest} message type, used to start job automatically.
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
	
	@Bean
	public JobLaunchingGateway launchJobGateway(JobLauncher jobLauncher) {
		JobLaunchingGateway launchJobGateway = new JobLaunchingGateway(jobLauncher);
		return launchJobGateway;
	}
	
	@Autowired
	private MailSendingMessageHandler mailSendingMessageHandler;
	
	@Bean
	public IntegrationFlow fileFlow(FileMessageToJobLaunchRequestTransformer fileToJobLaunchRequest, JobLaunchingGateway launchJobGateway, LoggingHandler loggingHandler) throws IOException {
		return IntegrationFlows.from(Files.inboundAdapter(watchedDirectoryResource.getFile()).autoCreateDirectory(true), spec -> spec.poller(Pollers.fixedDelay(5000)))
				.transform(fileToJobLaunchRequest)
				.handle(launchJobGateway)
				.transform(jobExecutionToMailMessage())
				.handle(mailSendingMessageHandler)
				.get();
	}
	
	@Bean
	public GenericTransformer<JobExecution, MailMessage> jobExecutionToMailMessage() {
		return new GenericTransformer<JobExecution, MailMessage>() {
			@Override
			public MailMessage transform(JobExecution source) {
				MailMessage mailMessage = new SimpleMailMessage();
				mailMessage.setFrom("zaky@jakartawebs.com");
				mailMessage.setReplyTo("zaky@jakartawebs.com");
				mailMessage.setTo("zakyalvan@gmail.com");
				mailMessage.setSubject("Execution of Job Summary");
				mailMessage.setText(String.format("Execution of you job (%s) finished with following details %s", source.getJobId(), source.toString()));
				mailMessage.setSentDate(Calendar.getInstance().getTime());
				return mailMessage;
			}
		};
	}
}
