package com.vinsguru.recommendation.service;

import com.vinsguru.netflux.events.MovieAddedEvent;
import com.vinsguru.recommendation.mapper.RecommendationMapper;
import com.vinsguru.recommendation.repository.MovieRepository;
import org.springframework.stereotype.Service;

@Service
public class MovieService {

    private final MovieRepository movieRepository;

    public MovieService(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    public void addMovie(MovieAddedEvent movieAddedEvent){
        var entity = RecommendationMapper.toMovie(movieAddedEvent);
        this.movieRepository.save(entity);
    }

}
