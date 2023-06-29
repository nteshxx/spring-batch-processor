package com.movies.processor.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	/*
	private final JdbcTemplate jdbcTemplate;

	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	*/

	@Override
	public void afterJob(JobExecution jobExecution) {
		if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
			//log.info("!!! JOB FINISHED! Time to verify the results");
			
			log.info("!!! JOB FINISHED!");
			
			/*
			jdbcTemplate.query("SELECT name, year FROM t_movies", 
					(rs, row) -> new Movie(rs.getString(1), rs.getString(2)))
				.forEach(movie -> log.info("Found <{{}}> in the database.", movie));
			*/
		}
	}
}
