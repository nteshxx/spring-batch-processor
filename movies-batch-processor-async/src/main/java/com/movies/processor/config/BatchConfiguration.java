package com.movies.processor.config;

import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
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
import org.springframework.integration.config.EnableIntegration;
import org.springframework.transaction.PlatformTransactionManager;

import com.movies.processor.entity.Movie;
import com.movies.processor.listener.JobCompletionNotificationListener;
import com.movies.processor.query.Queries;
import com.movies.processor.transformer.MovieItemProcessor;

@Configuration
@EnableIntegration
public class BatchConfiguration {
	
	@Value("${source}")
	private String fileSource;
	
	@Value("${chunk-size}")
	private int chunkSize;
	
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
	ItemProcessor<Movie, Movie> processor() {
		return new MovieItemProcessor();
	}
	
	@Bean
	AsyncItemProcessor<Movie, Movie> asyncProcessor(ItemProcessor<Movie, Movie> processor, TaskExecutor taskExecutor) {
		AsyncItemProcessor<Movie, Movie> asyncItemProcessor = new AsyncItemProcessor<>();
		asyncItemProcessor.setTaskExecutor(taskExecutor);
		asyncItemProcessor.setDelegate(processor);
		return asyncItemProcessor;
	}

	// WRITER
	@Bean
	@StepScope
	JdbcBatchItemWriter<Movie> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Movie>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql(Queries.INSERT_IN_T_MOVIES)
			.dataSource(dataSource)
			.build();
	}

	@Bean
	AsyncItemWriter<Movie> asyncWriter(JdbcBatchItemWriter<Movie> writer) {
		AsyncItemWriter<Movie> asyncItemWriter = new AsyncItemWriter<>();
		asyncItemWriter.setDelegate(writer);
		return asyncItemWriter;
	}

	// JOB
	@Bean
	Job importMoviesJob(JobRepository jobRepository, JobCompletionNotificationListener listener, Step step1) {
		return new JobBuilder("importMoviesJob", jobRepository)
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(step1)
			.end()
			.build();
	}

	// STEP
	@Bean
	Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, AsyncItemProcessor<Movie, Movie> asyncProcessor, AsyncItemWriter<Movie> asyncWriter) {
		return new StepBuilder("step1", jobRepository)
			.<Movie, Future<Movie>> chunk(chunkSize, transactionManager)
			.reader(reader())
			.processor(asyncProcessor)
			.writer(asyncWriter)
			.build();
	}

	// TASK EXECUTOR
	@Bean
	TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

}
