package com.vinsguru.movie;

import com.vinsguru.movie.dto.MovieDetails;
import com.vinsguru.netflux.events.MovieAddedEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import tools.jackson.databind.json.JsonMapper;

@AutoConfigureRestTestClient
@Import({
        TestcontainersConfiguration.class,
        MovieEventKafkaTest.TestConsumerConfiguration.class
})
@SpringBootTest(
        properties = {
                "app.import-movies=false",
                "spring.cloud.function.definition=testConsumer",
                "spring.cloud.stream.bindings.testConsumer-in-0.destination=movie-events",
                "spring.cloud.stream.kafka.binder.consumer-properties.key.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer",
                "spring.cloud.stream.kafka.binder.consumer-properties.auto.offset.reset=earliest"
        }
)
public class MovieEventKafkaTest {

    @Autowired
    private RestTestClient testClient;

    @Autowired
    private BlockingQueue<Message<MovieAddedEvent>> queue;

    @Test
    public void movieAddedEvent() throws InterruptedException {
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

        var message = this.queue.poll(5, TimeUnit.SECONDS);
        Assertions.assertNotNull(message);
        var event = message.getPayload();
        Assertions.assertEquals(response.id(), message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, Integer.class));
        Assertions.assertEquals(response.id(), event.movieId());
        Assertions.assertEquals("My Life Without Me", event.title());
    }

    @TestConfiguration
    static class TestConsumerConfiguration{

        @Bean
        public BlockingQueue<Message<MovieAddedEvent>> queue(){
            return new LinkedBlockingQueue<>();
        }

        @Bean
        Consumer<Message<MovieAddedEvent>> testConsumer(BlockingQueue<Message<MovieAddedEvent>> queue){
            return queue::add;
        }

    }

}
