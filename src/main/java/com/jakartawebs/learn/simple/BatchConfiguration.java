package com.jakartawebs.learn.simple;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	@Bean
	public ItemReader<Person> reader() {
		FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
		reader.setResource(new ClassPathResource("sample-data.csv"));
		reader.setLineMapper(new DefaultLineMapper<Person>() {{
			setLineTokenizer(new DelimitedLineTokenizer() {{
				setNames(new String[] {"firstName", "lastName"});
			}});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
				setTargetType(Person.class);
			}});
		}});
		return reader;
	}
	
	@Bean
	public ItemProcessor<Person, Person> processor() {
		return new PersonItemProcessor();
	}
	
	@Bean
	public ItemWriter<Person> writer(DataSource dataSource) {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
		writer.setDataSource(dataSource);
		return writer;
	}
	
	@Bean
	public Job importUserJob(JobBuilderFactory jobs, Step step, JobExecutionListener executionListener) {
		return jobs.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(executionListener)
				.flow(step)
				.end()
				.build();
	}
	
	@Bean
	public Step step1(StepBuilderFactory steps, ItemReader<Person> reader, ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {
		return steps.get("step1")
				.<Person, Person> chunk(10)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}
	
	@Bean
	public JdbcTemplate jdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}
	
	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor("import-user-job-thread"));
		return jobLauncher;
	}
}
