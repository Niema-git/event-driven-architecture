package ma.enset.kafka.handlers;

import ma.enset.kafka.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

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
}
