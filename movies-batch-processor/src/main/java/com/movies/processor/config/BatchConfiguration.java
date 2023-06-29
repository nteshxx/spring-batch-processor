package com.movies.processor.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.movies.processor.entity.Movie;
import com.movies.processor.listener.JobCompletionNotificationListener;
import com.movies.processor.repository.MovieRepository;
import com.movies.processor.transformer.MovieItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Value("${source}")
	private String fileSource;
	
	@Value("${chunk-size}")
	private int chunkSize;
	
	@Value("${thread-count}")
	private int threadCount;
	
	@Value("${maximum-pool-size}")
	private int maximumPoolSize;
	
	@Autowired
	private MovieRepository movieRepository;

	// tag::readerwriterprocessor[]
	// READER
	@Bean
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

	// PROCESSER/TRANSFORMER
	@Bean
	MovieItemProcessor processor() {
		return new MovieItemProcessor();
	}

	// use JdbcBatchWriter for Better Performance
	/*** using DataSource and JdbcBatchWriter ***
	@Bean
	JdbcBatchItemWriter<Movie> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<MovieNew>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql("INSERT INTO t_movies (name, year) VALUES (:name, :year)")
			.dataSource(dataSource)
			.build();
	}
	*/
	
	// WRITER
	@Bean
	RepositoryItemWriter<Movie> writer() {
		return new RepositoryItemWriterBuilder<Movie>()
			.repository(movieRepository)
			.methodName("save")
			.build();
	}
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
	Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, RepositoryItemWriter<Movie> writer, TaskExecutor taskExecutor) {
		return new StepBuilder("step1", jobRepository)
			.<Movie, Movie> chunk(chunkSize, transactionManager)
			.reader(reader())
			.processor(processor())
			.writer(writer)
			// TRANSACTION MANAGEMENT STEP LEVEL
			.transactionManager(transactionManager)
			// STEP LEVEL TASK EXECUTOR
			.taskExecutor(taskExecutor)
			.build();
	}
	// end::jobstep[]
	
	// TASK EXECUTOR
	/*** using custom executor
	@Bean
	TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(maximumPoolSize);
		//ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		// Set the number of threads
		//taskExecutor.setCorePoolSize(threadCount);
		//taskExecutor.setMaxPoolSize(maximumPoolSize);
		//taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}
	*/
	
	@Bean
	SimpleAsyncTaskExecutor simpleAsyncTaskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

}
