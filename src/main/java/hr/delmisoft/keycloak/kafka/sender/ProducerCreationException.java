package hr.delmisoft.keycloak.kafka.sender;

import java.io.Serial;

/**
 * The Kafka producer could not be constructed. Thrown at startup so Keycloak refuses to boot. The message
 * never contains producer property values (they may be secrets); the original exception is the cause.
 */
public class ProducerCreationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ProducerCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
