package com.jakartawebs.learn.simple;

import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SimpleBatchSampleApplication {
	public static void main(String[] args) throws NoSuchJobException, JobInstanceAlreadyExistsException, JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleBatchSampleApplication.class)
				.profiles("simple")
				.web(true)
				.run(args);
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> applicationContext.close()));
	}
}
