package hr.delmisoft.keycloak.kafka;

import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeContext;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.metrics.EventMetrics;
import hr.delmisoft.keycloak.kafka.outbox.OutboxCodec;
import hr.delmisoft.keycloak.kafka.outbox.OutboxWriter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransactionManager;

/**
 * Per-session listener. See CLAUDE.md "Key design rules". User events reach the instance cached by
 * {@code session.getProvider} (one per {@link KeycloakSession}); admin events reach instances that each
 * {@code AdminEventBuilder} creates with {@code factory.create(session)}, so one admin request can use several
 * instances, each with its own after-completion transaction. All of them commit or roll back with the session.
 *
 * <p>{@code onEvent} does all the work that needs the session and nothing that can block on Kafka: filter, compute
 * {@code source} ({@link EventSource}), map to the envelope, serialize, build the Kafka record, then
 * <ul>
 *   <li>async mode: buffer it in a {@link PublishAfterCommitTransaction}, which this instance enlists with the
 *       session's transaction manager on its first published event. When the transaction commits, the records go
 *       to the sender's bounded queue; on rollback they are discarded. Kafka I/O happens on the sender thread
 *       only;</li>
 *   <li>outbox mode: insert it into Keycloak's outbox table in the same transaction ({@link OutboxWriter}); the
 *       drainer task publishes it once committed.</li>
 * </ul>
 *
 * <p>Dispatch paths, none of which needs special handling:
 * <ul>
 *   <li>success events: the request session, inside the request transaction;</li>
 *   <li>error events: Keycloak's inner session ({@code KeycloakModelUtils.runJobInTransaction}), so another
 *       provider instance with its own transaction, published when that inner transaction commits;</li>
 *   <li>brute-force lockout events: Keycloak's {@code bruteforce} executor thread, with its own session and
 *       transaction;</li>
 *   <li>admin events: the admin request session.</li>
 * </ul>
 * Without an active transaction (none started, or already completed) there is nothing to wait for, and the
 * record is handed over immediately.
 *
 * <p>Never throws and never blocks: any failure is counted and logged at most once per minute without event
 * contents ({@link ListenerFailures}), and the event is not published.
 */
public final class KafkaEventListenerProvider implements EventListenerProvider {

    private static final String USER = Envelope.Kind.USER.value();
    private static final String ADMIN = Envelope.Kind.ADMIN.value();

    private final KeycloakSession session;
    private final ListenerPipeline pipeline;

    /** This session's buffer; created and enlisted on the first published event. */
    private PublishAfterCommitTransaction transaction;

    KafkaEventListenerProvider(KeycloakSession session, ListenerPipeline pipeline) {
        this.session = session;
        this.pipeline = pipeline;
    }

    @Override
    public void onEvent(Event event) {
        try {
            if (!pipeline.filter().accepts(event)) {
                pipeline.metrics().dropped(USER, EventMetrics.DROP_FILTERED);
                return;
            }
            EventSource.Resolved realm = EventSource.resolve(session, event.getRealmId(), event.getRealmName());
            Envelope envelope = pipeline.mapper().map(event, new EnvelopeContext(realm.source(), realm.realmName()));
            publish(envelope);
        } catch (RuntimeException | LinkageError e) {
            pipeline.failures().record(USER, "preparing user event " + (event == null ? null : event.getType()), e);
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        try {
            if (!pipeline.filter().accepts(event)) {
                pipeline.metrics().dropped(ADMIN, EventMetrics.DROP_FILTERED);
                return;
            }
            EventSource.Resolved realm = EventSource.resolve(session, event.getRealmId(), event.getRealmName());
            Envelope envelope = pipeline.mapper().map(event, includeRepresentation,
                    new EnvelopeContext(realm.source(), realm.realmName()));
            publish(envelope);
        } catch (RuntimeException | LinkageError e) {
            pipeline.failures().record(ADMIN, "preparing admin event "
                    + (event == null ? null : event.getResourceTypeAsString() + "." + event.getOperationType()), e);
        }
    }

    private void publish(Envelope envelope) {
        ProducerRecord<String, byte[]> record =
                pipeline.recordFactory().create(envelope, EnvelopeMapper.toJsonBytes(envelope));
        if (pipeline.outbox() != null) {
            pipeline.outbox().enqueue(session, OutboxCodec.encode(envelope, record));
        } else {
            publishAfterCommit(record);
        }
    }

    private void publishAfterCommit(ProducerRecord<String, byte[]> record) {
        if (transaction != null && transaction.isActive()) {
            transaction.add(record);
            return;
        }
        KeycloakTransactionManager manager = session.getTransactionManager();
        if (manager == null || !manager.isActive()) {
            pipeline.sink().offer(record);
            return;
        }
        PublishAfterCommitTransaction created = new PublishAfterCommitTransaction(pipeline.sink(), pipeline.failures());
        try {
            // Begins the transaction, since the manager is active.
            manager.enlistAfterCompletion(created);
        } catch (IllegalStateException alreadyCompleted) {
            // Still "active" while it runs its after-completion transactions, but completed: an event fired from
            // one of them reports work that is already committed.
            pipeline.sink().offer(record);
            return;
        }
        if (!created.isActive()) {
            created.begin();
        }
        created.add(record);
        transaction = created;
    }

    @Override
    public void close() {
        // Nothing to release: the transaction belongs to the session's transaction manager.
    }
}
