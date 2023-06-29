package com.movies.processor.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="t_movies")
public class Movie {

	@Id
    private String id;

	@Column
    private String name;
	
	@Column
    private String year;

	@Column
    private Float rating;
	
	@Column
    private String certificate;
	
	@Column
    private String duration;
	
	@Column
    private String genre;
	
	@Column
    private String votes;
	
	@Column
    private String gross_income;
	
	@Column(length=901)
    private String directors_id;
	
    @Column(length=1201)
    private String directors_name;
	
    @Column
    private String stars_id;
	
    @Column
    private String stars_name;
	
	@Column(length=501)
    private String description;
	
	public Movie(String name, String year) {
		this.name = name;
		this.year = year;
	}
	
}
