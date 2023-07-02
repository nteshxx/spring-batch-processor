package com.movies.processor.config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.movies.processor.entity.Movie;
import com.movies.processor.listener.JobCompletionNotificationListener;
import com.movies.processor.partitioner.CsvLinePartitioner;
import com.movies.processor.query.Queries;
import com.movies.processor.transformer.MovieItemProcessor;

@Configuration
public class BatchConfiguration {
	
	private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);
	
	@Value("${grid-size}")
	private Integer gridSize;
	
	@Value("${chunk-size}")
	private Integer chunkSize;
	
	@Value("${source}")
	private String fileSource;

	// PARTITIONER
	@Bean
	CsvLinePartitioner partitioner() {
		return new CsvLinePartitioner();
	}
	
	// READER
	@Bean
	@StepScope
	FlatFileItemReader<Movie> pagingItemReader(@Value("#{stepExecutionContext[startLine]}") Integer startLine, @Value("#{stepExecutionContext[pageSize]}") Integer pageSize) {
		log.debug("Start Line: " + (startLine) + ", Lines to read: " + pageSize);
		return new FlatFileItemReaderBuilder<Movie>()
				.name("pagingItemReader")
				.resource(new FileSystemResource(fileSource))
				.linesToSkip(startLine)
				.delimited()
				.names(new String[]{"id","name","year","rating","certificate","duration","genre","votes","gross_income","directors_id","directors_name","stars_id","stars_name","description"})
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Movie>() {{
					setTargetType(Movie.class);
				}})
				.maxItemCount(pageSize)
				.build();
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
	
	// MASTER
	@Bean
	@Primary
	Step masterStep(JobRepository jobRepository) {
		return new StepBuilder("masterStep", jobRepository)
				.partitioner(slaveStep(jobRepository, null, null).getName(), partitioner())
				.step(slaveStep(jobRepository, null, null))
				.gridSize((gridSize < 1) ? 1 : gridSize)
				.taskExecutor(taskExecutor())
				.build();
	}

	// SLAVE
	@Bean
	Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Movie> writer) {
		return new StepBuilder("slaveStep", jobRepository)
			.<Movie, Movie> chunk(chunkSize, transactionManager)
			.reader(pagingItemReader(null, null))
			//.processor(processor())
			.writer(writer)
			.build();
	}
	
	// JOB
	@Bean
	Job importMoviesJob(JobRepository jobRepository, JobCompletionNotificationListener listener, Step masterStep) {
		return new JobBuilder("importMoviesJob", jobRepository)
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(masterStep)
			.end()
			.build();
	}

	// PROCESSER
	@Bean
	MovieItemProcessor processor() {
		return new MovieItemProcessor();
	}

	// TASK EXECUTOR
	@Bean
	TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

}
