package ma.enset.kafka.handlers;

import ma.enset.kafka.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    // Consumer : lit les événements du topic R2 et les affiche
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return event -> {
            System.out.println("**************************");
            System.out.println(event);
            System.out.println("**************************");
        };
    }

    // Supplier : produit automatiquement un événement toutes les 200 ms dans le topic R3
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                10 + new Random().nextInt(1000));
    }

    // Kafka Streams : compte le nombre de visites de chaque page par fenêtre de 5 secondes
    // entrée : topic R3 (PageEvent) -> sortie : topic R4 (page, nombre de visites)
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return input -> input
                .filter((k, v) -> v.duration() > 100)
                .map((k, v) -> new KeyValue<>(v.name(), 0L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.as("page-count"))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v));
    }
}
