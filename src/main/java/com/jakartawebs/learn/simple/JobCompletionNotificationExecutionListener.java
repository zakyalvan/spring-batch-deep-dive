package com.jakartawebs.learn.simple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationExecutionListener extends JobExecutionListenerSupport {
	private static final Logger LOGGER = LoggerFactory.getLogger(JobCompletionNotificationExecutionListener.class);
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	public JobCompletionNotificationExecutionListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
			LOGGER.info("Job execution finished. Time to verify the results");
			List<Person> results = jdbcTemplate.query("SELECT first_name, last_name FROM people", new RowMapper<Person>() {
				@Override
				public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
					return new Person(rs.getString(1), rs.getString(2));
				}
			});
			
			results.forEach(person -> LOGGER.info("Found {}", person));
		}
	}	
}
