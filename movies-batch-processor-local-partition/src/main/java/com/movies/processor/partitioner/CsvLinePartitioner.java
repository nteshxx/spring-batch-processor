package com.movies.processor.partitioner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;

public class CsvLinePartitioner implements Partitioner {

	private static final Logger log = LoggerFactory.getLogger(CsvLinePartitioner.class);

	private static final String START_LINE = "startLine";
	private static final String PAGE_SIZE = "pageSize";

	@Value("${source}")
	private String fileSource;

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> partitionMap = new HashMap<>();

		// if gridSize is 0 or less than zero
		gridSize = (gridSize < 1) ? 1 : gridSize;

		int totalLines = getTotalLines();
		int pageSize = totalLines / gridSize;
		int remainingLines = totalLines % gridSize;

		int startLine = 1;
		for (int i = 0; i < gridSize; i++) {
			ExecutionContext executionContext = new ExecutionContext();

			int currentPageSize = pageSize;
			if (i < remainingLines) {
				// Adjust the page size for the last partition
				currentPageSize++;
			}

			// In case: totalLines < gridSize
			// to avoid unnecessary partition with 0 pageSize
			if (currentPageSize > 0) {
				executionContext.putInt(START_LINE, startLine);
				executionContext.putInt(PAGE_SIZE, currentPageSize);
				partitionMap.put("partition" + i, executionContext);
			}

			startLine += currentPageSize;
		}

		log.info("Partitions generated: " + partitionMap.toString());
		return partitionMap;
	}

	private Integer getTotalLines() {
		Integer lineCount = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(fileSource))) {
			while (reader.readLine() != null) {
				lineCount++;
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		log.info("Total CSV line count: " + lineCount);
		return lineCount;
	}

}
