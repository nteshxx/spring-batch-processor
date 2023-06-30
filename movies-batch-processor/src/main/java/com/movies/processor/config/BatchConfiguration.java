package com.movies.processor.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.movies.processor.entity.Movie;
import com.movies.processor.listener.JobCompletionNotificationListener;
import com.movies.processor.transformer.MovieItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Value("${source}")
	private String fileSource;
	
	@Value("${chunk-size}")
	private int chunkSize;
	
	// @Autowired
	// private MovieRepository movieRepository;
	
	// tag::readerwriterprocessor[]
	// READER
	@Bean
	@StepScope
	FlatFileItemReader<Movie> reader() {
		return new FlatFileItemReaderBuilder<Movie>()
			.name("movieItemReader")
			.resource(new FileSystemResource(fileSource))
			.delimited()
			.names(new String[]{"id","name","year","rating","certificate","duration","genre","votes","gross_income","directors_id","directors_name","stars_id","stars_name","description"})
			.fieldSetMapper(new BeanWrapperFieldSetMapper<Movie>() {{
				setTargetType(Movie.class);
			}})
			.linesToSkip(1)
			.build();
	}
	
	// PROCESSER
	@Bean
	MovieItemProcessor processor() {
		return new MovieItemProcessor();
	}
	
	// JdbcBatchWriter gives Better Performance than RepositoryItemWriter
	@Bean
	@StepScope
	JdbcBatchItemWriter<Movie> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Movie>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql("INSERT INTO t_movies (certificate, description, directors_id, directors_name, duration, genre, gross_income, name, rating, stars_id, stars_name, votes, year, id) VALUES (:certificate, :description, :directors_id, :directors_name, :duration, :genre, :gross_income, :name, :rating, :stars_id, :stars_name, :votes, :year, :id)")
			.dataSource(dataSource)
			.build();
	}
	
	// RepositoryItemWriter
	/*
	@Bean
	@StepScope
	RepositoryItemWriter<Movie> writer() {
		return new RepositoryItemWriterBuilder<Movie>()
			.repository(movieRepository)
			.methodName("save")
			.build();
	}
	*/
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	// JOB
	@Bean
	Job importMoviesJob(JobRepository jobRepository, JobCompletionNotificationListener listener, Step step1) {
		return new JobBuilder("importMoviesJob", jobRepository)
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(step1)
			//.next(step2)
			.end()
			.build();
	}
	
	// STEPS
	@Bean
	Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Movie> writer) {
		return new StepBuilder("step1", jobRepository)
			.<Movie, Movie> chunk(chunkSize, transactionManager)
			.reader(reader())
			.processor(processor())
			.writer(writer)
			.taskExecutor(taskExecutor())
			.build();
	}
	// end::jobstep[]
	
	// TASK EXECUTOR
	@Bean
	TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}
	
}
