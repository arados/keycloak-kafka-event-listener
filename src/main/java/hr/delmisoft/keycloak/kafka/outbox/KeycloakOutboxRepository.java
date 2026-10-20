package hr.delmisoft.keycloak.kafka.outbox;

import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.outbox.OutboxStore;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.jpa.entities.OutboxEntryEntity;
import org.keycloak.models.jpa.entities.OutboxEntryStatus;

/**
 * {@link OutboxRepository} on Keycloak's generic outbox: the {@code OUTBOX_ENTRY} table (Keycloak 26.7+, created
 * by Keycloak's own database migration, independent of the SSF feature) through
 * {@code org.keycloak.events.outbox.OutboxStore}. These are Keycloak-internal classes, not a public SPI, so this
 * class is the only place that touches them; the {@code bump-keycloak} skill re-checks them on every upgrade.
 *
 * <p>Rows are scoped by {@code ENTRY_KIND = }{@value #ENTRY_KIND}, so they never mix with SSF's rows. The owner is
 * the realm id, the correlation id the envelope {@code id} (OutboxStore deduplicates on kind + owner +
 * correlation id), the entry type the CloudEvents {@code type}.
 */
public final class KeycloakOutboxRepository implements OutboxRepository {

    /** {@code OUTBOX_ENTRY.ENTRY_KIND} of this listener's rows. */
    public static final String ENTRY_KIND = "kafka-events";

    /** Column widths of {@code OUTBOX_ENTRY} (Keycloak 26.7.0 changelog). */
    static final int MAX_REALM_ID = 36;
    static final int MAX_CORRELATION_ID = 255;
    static final int MAX_ENTRY_TYPE = 256;

    private final KeycloakSession session;
    private final OutboxStore store;
    /** Rows locked by {@link #lockDue} in this transaction, by id. */
    private final Map<String, OutboxEntryEntity> locked = new HashMap<>();

    public KeycloakOutboxRepository(KeycloakSession session) {
        this.session = session;
        this.store = new OutboxStore(session);
    }

    @Override
    public void enqueue(OutboxEntry entry) {
        store.enqueuePending(ENTRY_KIND, entry.realmId(), entry.realmId(), null, entry.eventId(), entry.type(),
                entry.payload(), entry.metadata());
    }

    @Override
    public List<OutboxRow> lockDue(int limit) {
        List<OutboxEntryEntity> rows = store.lockDueForDrain(ENTRY_KIND, limit);
        rows.forEach(row -> locked.put(row.getId(), row));
        return rows.stream()
                .map(row -> new OutboxRow(row.getId(), row.getCorrelationId(), row.getPayload(), row.getMetadata(),
                        row.getAttempts(), row.getCreatedAt()))
                .toList();
    }

    @Override
    public void markDelivered(String id) {
        store.markDelivered(entity(id));
    }

    @Override
    public void delete(String id) {
        entityManager().remove(entity(id));
    }

    @Override
    public void recordFailure(String id, Instant nextAttemptAt, String error) {
        store.recordFailure(entity(id), nextAttemptAt, error);
    }

    @Override
    public void retryLater(String id, Instant nextAttemptAt, String error) {
        OutboxEntryEntity row = entity(id);
        row.setNextAttemptAt(nextAttemptAt);
        row.setLastError(truncate(error));
    }

    @Override
    public void markDeadLetter(String id, String error) {
        store.markDeadLetter(entity(id), error);
    }

    @Override
    public int deadLetterPendingCreatedBefore(Instant cutoff, String reason) {
        return store.promoteStaleQueuedToDeadLetter(ENTRY_KIND, cutoff, reason);
    }

    @Override
    public void purgeDeliveredBefore(Instant cutoff) {
        store.purgeDeliveredOlderThan(ENTRY_KIND, cutoff);
    }

    @Override
    public void purgeDeadLettersBefore(Instant cutoff) {
        store.purgeDeadLetterOlderThan(ENTRY_KIND, cutoff);
    }

    @Override
    public OutboxStats stats() {
        List<Object[]> rows = entityManager()
                .createQuery("SELECT e.status, COUNT(e), MIN(e.createdAt) FROM OutboxEntryEntity e"
                        + " WHERE e.entryKind = :entryKind GROUP BY e.status", Object[].class)
                .setParameter("entryKind", ENTRY_KIND)
                .getResultList();
        long pending = 0;
        long deadLetters = 0;
        Instant oldestPending = null;
        for (Object[] row : rows) {
            OutboxEntryStatus status = (OutboxEntryStatus) row[0];
            long count = ((Number) row[1]).longValue();
            if (status == OutboxEntryStatus.PENDING) {
                pending = count;
                oldestPending = (Instant) row[2];
            } else if (status == OutboxEntryStatus.DEAD_LETTER) {
                deadLetters = count;
            }
        }
        return new OutboxStats(pending, deadLetters, oldestPending);
    }

    private OutboxEntryEntity entity(String id) {
        OutboxEntryEntity row = locked.get(id);
        return row != null ? row : store.findById(id);
    }

    private EntityManager entityManager() {
        return session.getProvider(JpaConnectionProvider.class).getEntityManager();
    }

    private static String truncate(String error) {
        return error == null || error.length() <= OutboxStore.MAX_LAST_ERROR_LENGTH
                ? error
                : error.substring(0, OutboxStore.MAX_LAST_ERROR_LENGTH);
    }
}
