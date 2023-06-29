package com.movies.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MoviesBatchProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(MoviesBatchProcessorApplication.class, args);
		//System.exit(SpringApplication.exit(SpringApplication.run(BatchProcessingApplication.class, args)));
	}

}
