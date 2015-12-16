package com.jakartawebs.learn.integration;

import java.beans.PropertyEditor;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BatchIntegrationSampleApplication {
	public static void main(String[] args) {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(BatchIntegrationSampleApplication.class)
				.profiles("integration")
				.web(true)
				.run(args);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> applicationContext.close()));
	}
	
	@Bean
	public CustomEditorConfigurer editorConfigurer() {
		CustomEditorConfigurer editorConfigurer = new CustomEditorConfigurer();
		
		Map<Class<?>, Class<? extends PropertyEditor>> customEditors = new  HashMap<>();
		customEditors.put(Range[].class, RangeArrayPropertyEditor.class);
		editorConfigurer.setCustomEditors(customEditors);
		
		return editorConfigurer;
	}
	
	@Bean
	@ConfigurationPropertiesBinding
	public DelegatingRangeArrayConverter rangeArrayConverter() {
		return new DelegatingRangeArrayConverter();
	}
	
	@Bean
	@ConfigurationProperties(prefix="violet.user.import.file.tokenizer")
	public FixedLengthTokenizerSettings userFileSettings() {
		return new FixedLengthTokenizerSettings();
	}
}
