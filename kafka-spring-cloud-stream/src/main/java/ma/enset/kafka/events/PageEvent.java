package ma.enset.kafka.events;

import java.util.Date;

// Événement : un utilisateur visite une page pendant une certaine durée (ms)
public record PageEvent(String name, String user, Date date, long duration) {
}
