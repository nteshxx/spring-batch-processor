package com.movies.processor.classifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.movies.processor.entity.Series;

public class SeriesClassifier implements ItemProcessor<Series, Series> {

	private static final Logger log = LoggerFactory.getLogger(MovieClassifier.class);

	@Override
	public Series process(final Series series) throws Exception {
		// perform operations on movie before inserting into DB
		boolean isSeries = series.getYear().split("-").length > 1;
		
		if (isSeries) {
			log.info("Inserting (" + series + ") into T_SERIES");
			return series;
		}
		
		return null;
	}

}
