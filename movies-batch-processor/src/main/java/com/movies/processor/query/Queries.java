package com.movies.processor.query;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Queries {

	public static final String INSERT_IN_T_MOVIES = "INSERT INTO t_movies (certificate, description, directors_id, directors_name, duration, genre, gross_income, name, rating, stars_id, stars_name, votes, year, id) VALUES (:certificate, :description, :directors_id, :directors_name, :duration, :genre, :gross_income, :name, :rating, :stars_id, :stars_name, :votes, :year, :id)";
	
}
