package com.movies.processor.transformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.movies.processor.entity.Movie;

public class MovieItemProcessor implements ItemProcessor<Movie, Movie> {

	private static final Logger log = LoggerFactory.getLogger(MovieItemProcessor.class);

	@Override
	public Movie process(final Movie movie) throws Exception {
		//final String name = movie.getName().toUpperCase();
		//final String year = movie.getYear().trim();

		//final Movie transformedMovie = new Movie(name, year);
		// TRANSFORMATION STEP
		// perform operations on movie before inserting into DB
		log.info("Inserting (" + movie + ") into T_MOVIES");

		//return transformedMovie;
		return movie;
	}

}
