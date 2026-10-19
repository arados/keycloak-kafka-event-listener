package hr.delmisoft.keycloak.kafka.config;

import hr.delmisoft.keycloak.kafka.KafkaEventsConst;
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

    /**
     * The standard "invalid option value" error.
     *
     * @param key    option key without prefix, e.g. {@code topic-events}; the message names it the way an
     *               operator types it ({@code spi-events-listener--kafka-events--topic-events})
     * @param value  the rejected value; never pass the value of a sensitive producer property
     * @param reason why it was rejected
     */
    public static KafkaEventsConfigException invalidValue(String key, String value, String reason) {
        return new KafkaEventsConfigException("Invalid value for " + KafkaEventsConst.OPTION_PREFIX + key
                + ": '" + value + "' (" + reason + ")");
    }
}
