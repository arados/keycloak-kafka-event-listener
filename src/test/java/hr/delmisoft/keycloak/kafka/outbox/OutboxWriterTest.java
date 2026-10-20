package hr.delmisoft.keycloak.kafka.outbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import hr.delmisoft.keycloak.kafka.metrics.MicrometerEventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.services.DefaultKeycloakTransactionManager;
import org.keycloak.tracing.NoopTracingProvider;
import org.keycloak.tracing.TracingProvider;

class OutboxWriterTest {

    private static final OutboxEntry ENTRY = new OutboxEntry("realm-1", "event-1", "org.keycloak.event.LOGIN", "user",
            "{}", "{\"topic\":\"t\"}");

    private final KeycloakSession session = mock(KeycloakSession.class);
    private final KeycloakSession ownSession = mock(KeycloakSession.class);
    private final KeycloakSessionFactory sessionFactory = mock(KeycloakSessionFactory.class);
    private final DefaultKeycloakTransactionManager transactions = new DefaultKeycloakTransactionManager(session);
    private final InMemoryOutboxRepository table = new InMemoryOutboxRepository();
    private final List<KeycloakSession> usedSessions = new ArrayList<>();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final OutboxWriter writer = new OutboxWriter(s -> {
        usedSessions.add(s);
        return table;
    }, new MicrometerEventMetrics(registry), (factory, task) -> task.run(ownSession));

    OutboxWriterTest() {
        when(session.getTransactionManager()).thenReturn(transactions);
        when(session.getKeycloakSessionFactory()).thenReturn(sessionFactory);
        when(session.getProvider(TracingProvider.class)).thenReturn(new NoopTracingProvider());
    }

    @Test
    void writesInTheActiveTransaction() {
        transactions.begin();

        writer.enqueue(session, ENTRY);

        assertThat(usedSessions, contains(session));
        assertThat(table.entries(), contains(ENTRY));
        assertThat(registry.get("keycloak.kafka.events.enqueued").tag("kind", "user").counter().count(),
                equalTo(1.0));
    }

    @Test
    void withoutAnActiveTransactionWritesInItsOwn() {
        writer.enqueue(session, ENTRY);

        assertThat(usedSessions, contains(ownSession));
        assertThat(table.entries(), hasSize(1));
    }

    @Test
    void duringAfterCompletionWritesInItsOwnTransaction() {
        transactions.begin();
        List<KeycloakSession> seen = new ArrayList<>();
        transactions.enlistAfterCompletion(new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
                // Keycloak runs this after the request's work committed; an event fired here must not be lost.
                writer.enqueue(session, ENTRY);
                seen.addAll(usedSessions);
            }

            @Override
            protected void rollbackImpl() {
            }
        });

        transactions.commit();

        assertThat(seen, contains(ownSession));
    }
}
