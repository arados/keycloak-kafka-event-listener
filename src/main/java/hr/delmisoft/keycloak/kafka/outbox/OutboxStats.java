package hr.delmisoft.keycloak.kafka.outbox;

import java.time.Instant;

/**
 * Outbox row counts of this listener.
 *
 * @param pending               rows waiting for delivery
 * @param deadLetters           rows given up on and not purged yet
 * @param oldestPendingCreatedAt creation time of the oldest pending row; {@code null} when none
 */
public record OutboxStats(long pending, long deadLetters, Instant oldestPendingCreatedAt) {

    public static final OutboxStats EMPTY = new OutboxStats(0, 0, null);
}
