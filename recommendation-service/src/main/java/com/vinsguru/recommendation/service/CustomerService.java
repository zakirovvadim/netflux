package com.vinsguru.recommendation.service;

import com.vinsguru.netflux.events.CustomerGenreUpdatedEvent;
import com.vinsguru.recommendation.mapper.RecommendationMapper;
import com.vinsguru.recommendation.repository.CustomerGenreRepository;
import org.springframework.stereotype.Service;

@Service
public class CustomerService {

    private final CustomerGenreRepository customerGenreRepository;

    public CustomerService(CustomerGenreRepository customerGenreRepository) {
        this.customerGenreRepository = customerGenreRepository;
    }

    public void updateGenre(CustomerGenreUpdatedEvent genreUpdatedEvent){
        var entity = RecommendationMapper.toCustomerGenre(genreUpdatedEvent);
        this.customerGenreRepository.save(entity);
    }

}
