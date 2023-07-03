package com.movies.processor.classifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.movies.processor.entity.Movie;

public class MovieClassifier implements ItemProcessor<Movie, Movie> {

	private static final Logger log = LoggerFactory.getLogger(MovieClassifier.class);

	@Override
	public Movie process(final Movie movie) throws Exception {
		// perform operations on movie before inserting into DB
		boolean isMovie = movie.getYear().split("-").length == 1;
		
		if (isMovie) {
			log.info("Inserting (" + movie + ") into T_MOVIES");
			return movie;
		}
		
		return null;
	}

}
