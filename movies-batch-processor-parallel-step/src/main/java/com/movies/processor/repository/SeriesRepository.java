package com.movies.processor.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.movies.processor.entity.Series;

public interface SeriesRepository extends JpaRepository<Series, String> {

}
