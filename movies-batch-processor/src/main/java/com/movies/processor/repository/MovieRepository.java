package com.movies.processor.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.movies.processor.entity.Movie;

public interface MovieRepository extends JpaRepository<Movie, String> {

}
