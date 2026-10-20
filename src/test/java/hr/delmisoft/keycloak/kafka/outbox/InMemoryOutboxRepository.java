package hr.delmisoft.keycloak.kafka.outbox;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** {@link OutboxRepository} over a map, with the state transitions of the real one; for unit tests. */
public final class InMemoryOutboxRepository implements OutboxRepository {

    public enum Status { PENDING, DELIVERED, DEAD_LETTER }

    /** One row; mutable so tests can inspect and arrange state. */
    public static final class Row {
        public final String id;
        public final OutboxEntry entry;
        public Status status = Status.PENDING;
        public int attempts;
        public Instant nextAttemptAt;
        public Instant createdAt;
        public Instant deliveredAt;
        public String lastError;

        Row(String id, OutboxEntry entry, Instant createdAt) {
            this.id = id;
            this.entry = entry;
            this.createdAt = createdAt;
            this.nextAttemptAt = createdAt;
        }
    }

    private final Map<String, Row> rows = new LinkedHashMap<>();
    private final List<OutboxEntry> entries = new ArrayList<>();
    private Instant now = Instant.parse("2026-10-20T12:00:00Z");
    private int nextId = 1;

    /** Everything enqueued, in order (also deduplicated ones are not repeated). */
    public List<OutboxEntry> entries() {
        return entries;
    }

    public Map<String, Row> rows() {
        return rows;
    }

    public Row row(String id) {
        return rows.get(id);
    }

    /** The clock {@link #lockDue} compares {@code nextAttemptAt} with, and new rows' creation time. */
    public void now(Instant now) {
        this.now = now;
    }

    @Override
    public void enqueue(OutboxEntry entry) {
        boolean duplicate = rows.values().stream().anyMatch(r -> r.entry.eventId().equals(entry.eventId()));
        if (!duplicate) {
            String id = "row-" + nextId++;
            rows.put(id, new Row(id, entry, now));
            entries.add(entry);
        }
    }

    @Override
    public List<OutboxRow> lockDue(int limit) {
        return rows.values().stream()
                .filter(r -> r.status == Status.PENDING && !r.nextAttemptAt.isAfter(now))
                .sorted(Comparator.comparing(r -> r.nextAttemptAt))
                .limit(limit)
                .map(r -> new OutboxRow(r.id, r.entry.eventId(), r.entry.payload(), r.entry.metadata(), r.attempts,
                        r.createdAt))
                .toList();
    }

    @Override
    public void markDelivered(String id) {
        Row row = rows.get(id);
        row.attempts++;
        row.status = Status.DELIVERED;
        row.deliveredAt = now;
        row.lastError = null;
    }

    @Override
    public void delete(String id) {
        rows.remove(id);
    }

    @Override
    public void recordFailure(String id, Instant nextAttemptAt, String error) {
        Row row = rows.get(id);
        row.attempts++;
        row.nextAttemptAt = nextAttemptAt;
        row.lastError = error;
    }

    @Override
    public void retryLater(String id, Instant nextAttemptAt, String error) {
        Row row = rows.get(id);
        row.nextAttemptAt = nextAttemptAt;
        row.lastError = error;
    }

    @Override
    public void markDeadLetter(String id, String error) {
        Row row = rows.get(id);
        row.attempts++;
        row.status = Status.DEAD_LETTER;
        row.lastError = error;
    }

    @Override
    public int deadLetterPendingCreatedBefore(Instant cutoff, String reason) {
        int count = 0;
        for (Row row : rows.values()) {
            if (row.status == Status.PENDING && row.createdAt.isBefore(cutoff)) {
                row.status = Status.DEAD_LETTER;
                row.lastError = reason;
                count++;
            }
        }
        return count;
    }

    @Override
    public void purgeDeliveredBefore(Instant cutoff) {
        rows.values().removeIf(r -> r.status == Status.DELIVERED && r.deliveredAt.isBefore(cutoff));
    }

    @Override
    public void purgeDeadLettersBefore(Instant cutoff) {
        rows.values().removeIf(r -> r.status == Status.DEAD_LETTER && r.createdAt.isBefore(cutoff));
    }

    @Override
    public OutboxStats stats() {
        List<Row> pending = rows.values().stream().filter(r -> r.status == Status.PENDING).toList();
        long dead = rows.values().stream().filter(r -> r.status == Status.DEAD_LETTER).count();
        Instant oldest = pending.stream().map(r -> r.createdAt).min(Comparator.naturalOrder()).orElse(null);
        return new OutboxStats(pending.size(), dead, oldest);
    }
}
