package hr.delmisoft.keycloak.kafka.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of {@code delivery-mode=outbox}; parsed (with defaults) whatever the mode, used only in outbox mode.
 *
 * @param drainInterval       {@code outbox-drain-interval-ms}: how often a node drains the outbox; at least 1 s
 * @param batchSize           {@code outbox-batch-size}: rows sent per drain run
 * @param maxAttempts         {@code outbox-max-attempts}: attempts for events Kafka rejects (e.g. not
 *                            authorized) before they are dead-lettered; Kafka being unreachable never counts
 * @param deliveredRetention  {@code outbox-delivered-retention-ms}: how long delivered rows are kept;
 *                            zero deletes them on delivery
 * @param deadLetterRetention {@code outbox-dead-letter-retention-ms}: how long dead-lettered rows are kept,
 *                            counted from the event's creation; zero keeps them forever
 * @param pendingMaxAge       {@code outbox-pending-max-age-ms}: rows still undelivered after this are
 *                            dead-lettered; zero disables the backstop
 */
public record OutboxOptions(
        Duration drainInterval,
        int batchSize,
        int maxAttempts,
        Duration deliveredRetention,
        Duration deadLetterRetention,
        Duration pendingMaxAge) {

    public OutboxOptions {
        Objects.requireNonNull(drainInterval, "drainInterval");
        Objects.requireNonNull(deliveredRetention, "deliveredRetention");
        Objects.requireNonNull(deadLetterRetention, "deadLetterRetention");
        Objects.requireNonNull(pendingMaxAge, "pendingMaxAge");
    }

    public static final OutboxOptions DEFAULTS = new OutboxOptions(
            Duration.ofMillis(1_000), 500, 10, Duration.ZERO, Duration.ofDays(14), Duration.ofDays(7));
}
