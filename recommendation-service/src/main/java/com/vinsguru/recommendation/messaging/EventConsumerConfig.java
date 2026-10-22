package com.vinsguru.recommendation.messaging;

import com.vinsguru.netflux.events.CustomerGenreUpdatedEvent;
import com.vinsguru.netflux.events.MovieAddedEvent;
import com.vinsguru.recommendation.service.CustomerService;
import com.vinsguru.recommendation.service.MovieService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(EventConsumerConfig.class);

    @Bean
    public Consumer<CustomerGenreUpdatedEvent> customerGenreUpdatedEventConsumer(CustomerService customerService) {
        return withLogging(customerService::updateGenre);
    }

    @Bean
    public Consumer<MovieAddedEvent> movieAddedEventConsumer(MovieService movieService) {
        return withLogging(movieService::addMovie);
    }

    private <T> Consumer<T> withLogging(Consumer<T> consumer) {
        return t -> {
            log.info("received: {}", t);
            consumer.accept(t);
        };
    }
}
