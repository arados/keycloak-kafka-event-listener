package hr.delmisoft.keycloak.kafka.outbox;

import hr.delmisoft.keycloak.kafka.metrics.EventMetrics;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.KeycloakSessionTask;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.utils.KeycloakModelUtils;

/**
 * Writes events into the outbox from {@code onEvent}, inside the transaction that dispatches the event: the row
 * commits or rolls back with the Keycloak work that produced the event (the request, Keycloak's inner transaction
 * for error events, the brute-force task). Thread-safe; one instance per factory.
 *
 * <p>When the session has no transaction that can still take work (none active, or Keycloak is already running
 * its after-completion transactions, i.e. the work is committed) the row is written in a transaction of its own.
 *
 * <p>This is a synchronous database insert on the calling thread, like Keycloak's own event store. A database
 * that rejects it fails the transaction just as it would fail Keycloak's own writes.
 */
public final class OutboxWriter {

    private final Function<KeycloakSession, OutboxRepository> repositories;
    private final EventMetrics metrics;
    private final BiConsumer<KeycloakSessionFactory, KeycloakSessionTask> ownTransaction;

    public OutboxWriter(Function<KeycloakSession, OutboxRepository> repositories, EventMetrics metrics) {
        this(repositories, metrics, KeycloakModelUtils::runJobInTransaction);
    }

    /** For tests: {@code ownTransaction} replaces {@link KeycloakModelUtils#runJobInTransaction}. */
    OutboxWriter(Function<KeycloakSession, OutboxRepository> repositories, EventMetrics metrics,
            BiConsumer<KeycloakSessionFactory, KeycloakSessionTask> ownTransaction) {
        this.repositories = Objects.requireNonNull(repositories, "repositories");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.ownTransaction = Objects.requireNonNull(ownTransaction, "ownTransaction");
    }

    public void enqueue(KeycloakSession session, OutboxEntry entry) {
        if (acceptsWork(session.getTransactionManager())) {
            repositories.apply(session).enqueue(entry);
        } else {
            ownTransaction.accept(session.getKeycloakSessionFactory(), own -> repositories.apply(own).enqueue(entry));
        }
        metrics.enqueued(entry.kind());
    }

    /**
     * Whether work done now still commits with the session's transaction. Keycloak's transaction manager stays
     * "active" while it runs after-completion transactions but refuses enlistment then; probing with a no-op
     * transaction tells the two states apart.
     */
    private static boolean acceptsWork(KeycloakTransactionManager manager) {
        if (manager == null || !manager.isActive()) {
            return false;
        }
        try {
            manager.enlistAfterCompletion(new NoOpTransaction());
            return true;
        } catch (IllegalStateException alreadyCompleted) {
            return false;
        }
    }

    private static final class NoOpTransaction extends AbstractKeycloakTransaction {

        @Override
        protected void commitImpl() {
            // probe only
        }

        @Override
        protected void rollbackImpl() {
            // probe only
        }
    }
}
