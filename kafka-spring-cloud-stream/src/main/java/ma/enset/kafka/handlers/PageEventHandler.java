package ma.enset.kafka.handlers;

import ma.enset.kafka.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
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
}
