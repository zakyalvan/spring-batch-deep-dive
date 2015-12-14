package com.jakartawebs.learn.integration;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class BatchIntegrationSampleApplication {
	public static void main(String[] args) {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(BatchIntegrationSampleApplication.class)
				.profiles("integration")
				.web(true)
				.run(args);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> applicationContext.close()));
	}
}
