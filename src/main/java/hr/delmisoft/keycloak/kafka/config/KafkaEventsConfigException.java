package hr.delmisoft.keycloak.kafka.config;

import java.io.Serial;

/**
 * Invalid listener configuration. Thrown at startup so Keycloak refuses to boot with a message naming
 * the offending option exactly as an operator types it. Messages never contain values of sensitive
 * Kafka producer properties.
 */
public class KafkaEventsConfigException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public KafkaEventsConfigException(String message) {
        super(message);
    }
}
