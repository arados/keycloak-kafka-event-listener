package hr.delmisoft.keycloak.kafka.outbox;

import java.time.Instant;
import java.util.List;

/**
 * Our view of the outbox table, so that only {@link KeycloakOutboxRepository} depends on Keycloak's internal
 * {@code org.keycloak.events.outbox} classes (which can change in any Keycloak minor release). One instance per
 * {@code KeycloakSession}; every call runs in that session's transaction.
 */
public interface OutboxRepository {

    /** Inserts a pending row; a row with the same event id is not inserted twice. */
    void enqueue(OutboxEntry entry);

    /**
     * Locks up to {@code limit} due pending rows, oldest first, for the rest of the transaction. Rows another
     * node holds are skipped.
     */
    List<OutboxRow> lockDue(int limit);

    /** Kafka acknowledged the row: keep it as delivered (purged after the retention). */
    void markDelivered(String id);

    /** Kafka acknowledged the row: delete it. */
    void delete(String id);

    /** Counts a failed attempt and schedules the next one. */
    void recordFailure(String id, Instant nextAttemptAt, String error);

    /** Schedules the next attempt without counting one (Kafka was unreachable; the row was never judged). */
    void retryLater(String id, Instant nextAttemptAt, String error);

    /** Gives up on the row. */
    void markDeadLetter(String id, String error);

    /** Dead-letters every pending row created before {@code cutoff}; returns how many. */
    int deadLetterPendingCreatedBefore(Instant cutoff, String reason);

    /** Deletes delivered rows delivered before {@code cutoff}. */
    void purgeDeliveredBefore(Instant cutoff);

    /** Deletes dead-lettered rows created before {@code cutoff}. */
    void purgeDeadLettersBefore(Instant cutoff);

    /** Row counts for the metrics gauges. */
    OutboxStats stats();
}
