package com.movies.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MoviesBatchProcessorLocalPartitionApplication {

	public static void main(String[] args) {
		SpringApplication.run(MoviesBatchProcessorLocalPartitionApplication.class, args);
	}

}
