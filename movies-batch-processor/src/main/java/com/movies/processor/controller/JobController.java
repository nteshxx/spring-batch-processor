package com.movies.processor.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.movies.processor.utility.ResponseBuilder;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private Job job;
	
	@PostMapping("/import-movies")
	public ResponseEntity<Object> importMoviesJob() {
		JobParameters jobParams = new JobParametersBuilder()
				.addLong("startAt", System.currentTimeMillis()).toJobParameters();
		
		try {
			jobLauncher.run(job, jobParams);
			return ResponseBuilder.build(HttpStatus.CREATED, "Job Executed Successfully", null, null);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseBuilder.build(HttpStatus.INTERNAL_SERVER_ERROR, "Job Failed", e.getMessage(), e.getStackTrace());
		}
	}

}
