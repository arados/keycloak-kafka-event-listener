package hr.delmisoft.keycloak.kafka.outbox;

/** A running outbox as the listener factory sees it ({@link KafkaOutbox} in production). */
public interface OutboxRuntime extends AutoCloseable {

    /** What providers write events with. */
    OutboxWriter writer();

    /** Stops publishing; undelivered rows stay in the outbox. Idempotent. */
    @Override
    void close();
}
