package com.vinsguru.movie;

import com.vinsguru.movie.dto.MovieDetails;
import com.vinsguru.movie.service.MovieDataInitializer;
import com.vinsguru.netflux.events.MovieAddedEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.EnableTestBinder;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.client.EntityExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import tools.jackson.databind.json.JsonMapper;


@EnableTestBinder
@AutoConfigureRestTestClient
@SpringBootTest(properties = "app.import-movies=false")
public class MovieEventBinderTest {

    @Autowired
    private RestTestClient testClient;

    @Autowired
    private OutputDestination outputDestination;

    @Test
    public void movieAddedEvent() {
        var json = """
                {"title":"My Life Without Me","voteAverage":5.941,"voteCount":421,"releaseDate":"2003-03-07","revenue":12300000,"runtime":106,"backdropPath":"/kZyurQjTMLHalUxs7sHgH5XeiwO.jpg","budget":2500000,"homepage":"http://www.clubcultura.com/clubcine/clubcineastas/isabelcoixet/mividasinmi/index.htm","overview":"A fatally ill mother with only two months to live creates a list of things she wants to do before she dies without telling her family of her illness.","popularity":13.52,"posterPath":"/sFSkn5rrQqXJkRNa2rMWqzmEuhR.jpg","genres":["Drama","Romance"]}
                """;
        var request = JsonMapper.shared().readValue(json, MovieDetails.class);
        var response = this.testClient.post()
                .uri("/api/movies")
                .body(request)
                .exchange()
                .expectStatus().is2xxSuccessful()
                .returnResult(MovieDetails.class)
                .getResponseBody();

        Assertions.assertNotNull(response);
        Assertions.assertNotNull(response.id());
        Assertions.assertEquals("My Life Without Me", response.title());

        Message<byte[]> message = this.outputDestination.receive(1000, "movie-events");
        var event = JsonMapper.shared().readValue(message.getPayload(), MovieAddedEvent.class);
        Assertions.assertEquals(response.id(), message.getHeaders().get(KafkaHeaders.KEY, Integer.class));
        Assertions.assertEquals(response.id(), event.movieId());
        Assertions.assertEquals("My Life Without Me", event.title());
    }

}
