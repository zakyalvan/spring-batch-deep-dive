package com.jakartawebs.learn.simple;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/users/import-job")
public class ImportJobLaunchingController {
	private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobLaunchingController.class);
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	@Qualifier("importUserJob")
	private Job importUserJob;
	
	@RequestMapping(method=RequestMethod.POST)
	public JobExecutionInfo startJob() throws Exception {
		LOGGER.info("Starting batch import user job");
		JobExecution jobExecution = jobLauncher.run(importUserJob, new JobParameters());
		return new JobExecutionInfo(jobExecution);
	}
	
	public static class JobExecutionInfo {
		private Long id;
		private BatchStatus batchStatus;
		private ExitStatus exitStatus;
		
		public JobExecutionInfo(JobExecution jobExecution) {
			this.id = jobExecution.getId();
			this.batchStatus = jobExecution.getStatus();
			this.exitStatus = jobExecution.getExitStatus();
		}

		public Long getId() {
			return id;
		}
		public void setId(Long id) {
			this.id = id;
		}

		public BatchStatus getBatchStatus() {
			return batchStatus;
		}
		public void setBatchStatus(BatchStatus batchStatus) {
			this.batchStatus = batchStatus;
		}

		public ExitStatus getExitStatus() {
			return exitStatus;
		}
		public void setExitStatus(ExitStatus exitStatus) {
			this.exitStatus = exitStatus;
		}		
	}
}
