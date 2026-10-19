package hr.delmisoft.keycloak.kafka.config;

/** How events reach Kafka ({@code delivery-mode} option). */
public enum DeliveryMode {

    /**
     * At-most-once: after the Keycloak transaction commits, events go to a bounded in-memory queue drained
     * by a sender thread. {@code outbox} (at-least-once) is reserved for milestone M3.
     */
    ASYNC
}
