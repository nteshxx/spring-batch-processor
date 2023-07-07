package com.movies.processor.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
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

import com.movies.processor.classifier.MovieClassifier;
import com.movies.processor.classifier.SeriesClassifier;
import com.movies.processor.entity.Movie;
import com.movies.processor.entity.Series;
import com.movies.processor.listener.JobCompletionNotificationListener;
import com.movies.processor.query.Queries;

@Configuration
public class BatchConfiguration {
	
	@Value("${source}")
	private String fileSource;
	
	@Value("${chunk-size}")
	private int chunkSize;
	
	// READERS
	@Bean
	@StepScope
	FlatFileItemReader<Movie> movieReader() {
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
	
	@Bean
	@StepScope
	FlatFileItemReader<Series> seriesReader() {
		return new FlatFileItemReaderBuilder<Series>()
			.name("seriesItemReader")
			.resource(new FileSystemResource(fileSource))
			.delimited()
			.names(new String[]{"id","name","year","rating","certificate","duration","genre","votes","gross_income","directors_id","directors_name","stars_id","stars_name","description"})
			.fieldSetMapper(new BeanWrapperFieldSetMapper<Series>() {{
				setTargetType(Series.class);
			}})
			.linesToSkip(1)
			.build();
	}
	
	// PROCESSERS
	@Bean
	MovieClassifier moviesClassifier() {
		return new MovieClassifier();
	}
	
	@Bean
	SeriesClassifier seriesClassifier() {
		return new SeriesClassifier();
	}
	
	// WRITERS
	@Bean
	@StepScope
	JdbcBatchItemWriter<Movie> movieWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Movie>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql(Queries.INSERT_IN_T_MOVIES)
			.dataSource(dataSource)
			.build();
	}
	
	@Bean
	@StepScope
	JdbcBatchItemWriter<Series> seriesWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Series>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql(Queries.INSERT_IN_T_SERIES)
			.dataSource(dataSource)
			.build();
	}
	
	// STEPS
	@Bean
	Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Movie> movieWriter) {
		return new StepBuilder("flow1", jobRepository)
			.<Movie, Movie> chunk(chunkSize, transactionManager)
			.reader(movieReader())
			.processor(moviesClassifier())
			.writer(movieWriter)
			.build();
	}
	
	@Bean
	Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Series> seriesWriter) {
		return new StepBuilder("flow2", jobRepository)
			.<Series, Series> chunk(chunkSize, transactionManager)
			.reader(seriesReader())
			.processor(seriesClassifier())
			.writer(seriesWriter)
			.build();
	}

	// TASK EXECUTOR
	@Bean
	TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

	// FLOWS
	@Bean
	Flow splitFlow(Flow flow1, Flow flow2) {
	    return new FlowBuilder<SimpleFlow>("splitFlow")
	        .split(taskExecutor())
	        .add(flow1, flow2)
	        .build();
	}

	@Bean
	Flow flow1(Step step1) {
	    return new FlowBuilder<SimpleFlow>("flow1")
	        .start(step1)
	        .build();
	}

	@Bean
	Flow flow2(Step step2) {
	    return new FlowBuilder<SimpleFlow>("flow2")
	        .start(step2)
	        .build();
	}
	
	// JOB
	@Bean
	Job importMoviesJob(JobRepository jobRepository, JobCompletionNotificationListener listener, Flow splitFlow) {
		return new JobBuilder("importMoviesJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(splitFlow).build()  	// build flow
				.build(); 					// build job
	}

}
