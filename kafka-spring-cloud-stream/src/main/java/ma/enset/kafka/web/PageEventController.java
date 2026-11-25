package ma.enset.kafka.web;

import ma.enset.kafka.events.PageEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
public class PageEventController {
    private final StreamBridge streamBridge;

    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
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
}
