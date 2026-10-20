package hr.delmisoft.keycloak.kafka;

import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.keycloak.models.AbstractKeycloakTransaction;

/**
 * Buffers one session's records and hands them to the sender only if that session's Keycloak transaction
 * commits. Enlisted with {@code KeycloakTransactionManager.enlistAfterCompletion}, so Keycloak runs it after
 * every regular transaction committed; if any of them failed, or the request rolled back, it is rolled back
 * instead and the records are discarded.
 *
 * <p>Why not Keycloak's {@code org.keycloak.events.EventListenerTransaction}: it buffers the
 * {@code Event}/{@code AdminEvent} objects and runs a consumer on them at commit time, so mapping would happen
 * in the after-commit callback. Computing {@code source} needs the session (realm, request URI), which must not
 * be touched there. This transaction holds records that {@code onEvent} already built, so commit only enqueues
 * bytes.
 *
 * <p>{@link #commitImpl()} must never throw: Keycloak rethrows exceptions from after-completion transactions
 * out of its commit, which would fail a request whose work is already committed. Not thread-safe; a session is
 * used by one thread at a time.
 */
final class PublishAfterCommitTransaction extends AbstractKeycloakTransaction {

    private final RecordSink sink;
    private final ListenerFailures failures;
    private final List<ProducerRecord<String, byte[]>> records = new ArrayList<>(2);

    PublishAfterCommitTransaction(RecordSink sink, ListenerFailures failures) {
        this.sink = sink;
        this.failures = failures;
    }

    void add(ProducerRecord<String, byte[]> record) {
        records.add(record);
    }

    @Override
    protected void commitImpl() {
        // No session access here, and no blocking: offer() only appends to the sender's bounded queue.
        try {
            for (ProducerRecord<String, byte[]> record : records) {
                try {
                    sink.offer(record);
                } catch (RuntimeException | LinkageError e) {
                    failures.record(EventRecordFactory.kindOf(record), "handing a committed event to the Kafka sender",
                            e);
                }
            }
        } finally {
            records.clear();
        }
    }

    @Override
    protected void rollbackImpl() {
        records.clear();
    }
}
