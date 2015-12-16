package com.jakartawebs.learn.integration;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.mail.MailSendingMessageHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.mail.MailMessage;
import org.springframework.mail.SimpleMailMessage;

@Configuration
public class ImportUserJobLaunchConfiguration {
	@Value("file://${user.home}/FTP/batch/integration")
	private Resource watchedDirectoryResource;
	
	@Bean(name="userFileToJobLaunchRequest")
	public GenericTransformer<File, JobLaunchRequest> userFileToJobLaunchRequest(@Qualifier("importJob") Job importUserJob, @Value("input.file.name") String fileParameterName) {
		return new GenericTransformer<File, JobLaunchRequest>() {
			@Override
			public JobLaunchRequest transform(File source) {
				JobParametersBuilder parametersBuilder = new JobParametersBuilder();
				parametersBuilder.addString(fileParameterName, source.getAbsolutePath());		
				return new JobLaunchRequest(importUserJob, parametersBuilder.toJobParameters());
			}
		};
	}
	
	@Bean
	public JobLaunchingGateway launchJobGateway(JobLauncher jobLauncher) {
		JobLaunchingGateway launchJobGateway = new JobLaunchingGateway(jobLauncher);
		return launchJobGateway;
	}
	
	@Bean(name="jobExecutionToMailMessage")
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
	
	@Bean
	public IntegrationFlow jobLaunchingFlow(JobLaunchingGateway launchJobGateway,
			@Qualifier("userFileToJobLaunchRequest") GenericTransformer<File, JobLaunchRequest> fileToJobLaunchRequest,
			@Qualifier("jobExecutionToMailMessage") GenericTransformer<JobExecution, MailMessage> jobExecutionToMailMessage,
			MailSendingMessageHandler mailSendingMessageHandler,
			LoggingHandler loggingHandler) throws IOException {
		return IntegrationFlows.from(Files.inboundAdapter(watchedDirectoryResource.getFile()).autoCreateDirectory(true), spec -> spec.poller(Pollers.fixedDelay(5000)))
				.transform(fileToJobLaunchRequest)
				.handle(launchJobGateway)
				.transform(jobExecutionToMailMessage)
				.handle(mailSendingMessageHandler)
				.get();
	}
}
