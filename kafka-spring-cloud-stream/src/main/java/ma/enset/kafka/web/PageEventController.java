package ma.enset.kafka.web;

import ma.enset.kafka.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {
    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;

    public PageEventController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    // Producer : publie un PageEvent dans le topic demandé
    // ex : http://localhost:8080/publish?name=P1&topic=R2
    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam String topic) {
        PageEvent event = new PageEvent(name, Math.random() > 0.5 ? "U1" : "U2",
                new Date(), 10 + new Random().nextInt(1000));
        streamBridge.send(topic, event);
        return event;
    }

    // Flux temps réel (Server-Sent Events) : chaque seconde, nombre de visites par page
    // sur les 5 dernières secondes, lu dans le store "page-count" de Kafka Streams
    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1)).map(seq -> {
            Map<String, Long> result = new HashMap<>();
            ReadOnlyWindowStore<String, Long> store = interactiveQueryService
                    .getQueryableStore("page-count", QueryableStoreTypes.windowStore());
            Instant now = Instant.now();
            try (KeyValueIterator<Windowed<String>, Long> it = store.fetchAll(now.minusMillis(5000), now)) {
                while (it.hasNext()) {
                    KeyValue<Windowed<String>, Long> next = it.next();
                    result.put(next.key.key(), next.value);
                }
            }
            return result;
        });
    }
}
