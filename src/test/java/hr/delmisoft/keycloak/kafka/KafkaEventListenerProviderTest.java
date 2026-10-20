package hr.delmisoft.keycloak.kafka;

import static hr.delmisoft.keycloak.kafka.KeycloakFixture.ISSUER;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.MASTER_ID;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.REALM_ID;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.USER_ID;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.adminEvent;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.header;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.userEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import hr.delmisoft.keycloak.kafka.envelope.DetailRedaction;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeOptions;
import hr.delmisoft.keycloak.kafka.routing.EventFilter;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import hr.delmisoft.keycloak.kafka.routing.KeyStrategy;
import hr.delmisoft.keycloak.kafka.routing.TopicRouter;
import jakarta.enterprise.context.ContextNotActiveException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.urls.UrlType;
import org.keycloak.util.JsonSerialization;

class KafkaEventListenerProviderTest {

    private final KeycloakFixture keycloak = new KeycloakFixture().begin();
    private final List<ProducerRecord<String, byte[]>> published = new ArrayList<>();
    private final ListenerFailures failures = new ListenerFailures();

    private RecordSink sink = published::add;
    private EventFilter filter = new EventFilter(Set.of(), Set.of(), Set.of(), Set.of());
    private EnvelopeOptions options = EnvelopeOptions.DEFAULTS;

    @Test
    void committedUserEventIsPublishedWithIssuerSourceKeyAndHeaders() throws IOException {
        provider().onEvent(userEvent(EventType.LOGIN));

        assertThat("nothing is handed over before the commit", published, empty());
        keycloak.transactions.commit();

        assertThat(published, hasSize(1));
        ProducerRecord<String, byte[]> record = published.get(0);
        assertThat(record.topic(), equalTo("keycloak.events"));
        assertThat(record.key(), equalTo(REALM_ID + ":" + USER_ID));
        assertThat(header(record.headers(), "content-type"), equalTo("application/cloudevents+json; charset=UTF-8"));
        assertThat(header(record.headers(), "kc-kind"), equalTo("user"));
        assertThat(header(record.headers(), "kc-event-type"), equalTo("org.keycloak.event.LOGIN"));
        assertThat(header(record.headers(), "kc-realm-id"), equalTo(REALM_ID));
        assertThat(header(record.headers(), "kc-schema-version"), equalTo("1"));
        JsonNode envelope = json(record);
        assertThat(envelope.get("source").asText(), equalTo(ISSUER));
        assertThat(envelope.get("type").asText(), equalTo("org.keycloak.event.LOGIN"));
        assertThat(envelope.get("kcrealmname").asText(), equalTo("kafka-demo"));
        assertThat(envelope.get("data").get("ipAddress").asText(), equalTo("192.0.2.10"));
    }

    @Test
    void rollbackDiscardsBufferedEvents() {
        var provider = provider();
        provider.onEvent(userEvent(EventType.LOGIN));
        provider.onEvent(adminEvent(ResourceType.USER, OperationType.CREATE, "users/1"), false);

        keycloak.transactions.rollback();

        assertThat(published, empty());
    }

    @Test
    void failedMainTransactionDiscardsEvents() {
        // Keycloak rolls after-completion transactions back when a regular transaction fails to commit.
        keycloak.transactions.enlist(new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
                throw new IllegalStateException("database said no");
            }

            @Override
            protected void rollbackImpl() {
            }
        });
        provider().onEvent(userEvent(EventType.LOGIN));

        assertThrows(IllegalStateException.class, keycloak.transactions::commit);

        assertThat(published, empty());
    }

    @Test
    void enlistsOnceAndPublishesAllEventsOfTheSessionInOrder() {
        var provider = provider();
        provider.onEvent(userEvent(EventType.LOGIN));
        provider.onEvent(adminEvent(ResourceType.USER, OperationType.UPDATE, "users/1"), false);
        provider.onEvent(userEvent(EventType.CODE_TO_TOKEN));

        verify(keycloak.transactions, times(1)).enlistAfterCompletion(any(KeycloakTransaction.class));
        keycloak.transactions.commit();

        assertThat(published.stream().map(r -> header(r.headers(), "kc-event-type")).toList(), contains(
                "org.keycloak.event.LOGIN", "org.keycloak.admin.USER.UPDATE", "org.keycloak.event.CODE_TO_TOKEN"));
    }

    @Test
    void filteredEventsNeverEnlist() {
        filter = new EventFilter(Set.of(), Set.of(EventType.LOGIN), Set.of(), Set.of("USER"));
        var provider = provider();

        provider.onEvent(userEvent(EventType.LOGIN));
        provider.onEvent(adminEvent(ResourceType.USER, OperationType.CREATE, "users/1"), true);

        verify(keycloak.transactions, never()).enlistAfterCompletion(any(KeycloakTransaction.class));
        keycloak.transactions.commit();
        assertThat(published, empty());
    }

    @Test
    void errorEventFromTheInnerSessionIsPublishedWhenTheInnerTransactionCommits() throws IOException {
        // EventBuilder.error() dispatches in KeycloakModelUtils.runJobInTransaction: a new session, so Keycloak
        // creates a separate provider whose transaction commits before the outer request's does.
        var outer = provider();
        outer.onEvent(userEvent(EventType.CODE_TO_TOKEN));
        KeycloakFixture inner = new KeycloakFixture().begin();
        var innerProvider = new KafkaEventListenerProvider(inner.session, pipeline());

        Event error = userEvent(EventType.LOGIN_ERROR);
        error.setError("invalid_user_credentials");
        innerProvider.onEvent(error);
        inner.transactions.commit();

        assertThat(published, hasSize(1));
        JsonNode envelope = json(published.get(0));
        assertThat(envelope.get("type").asText(), equalTo("org.keycloak.event.LOGIN_ERROR"));
        assertThat(envelope.get("source").asText(), equalTo(ISSUER));
        assertThat(envelope.get("data").get("error").asText(), equalTo("invalid_user_credentials"));

        // The failed request rolls back; the error event was committed on its own and stays published.
        keycloak.transactions.rollback();
        assertThat(published, hasSize(1));
    }

    @Test
    void withoutAnActiveTransactionTheRecordIsHandedOverImmediately() {
        KeycloakFixture notStarted = new KeycloakFixture();
        var provider = new KafkaEventListenerProvider(notStarted.session, pipeline());

        provider.onEvent(userEvent(EventType.LOGIN));

        assertThat(published, hasSize(1));
        verify(notStarted.transactions, never()).enlistAfterCompletion(any(KeycloakTransaction.class));
    }

    @Test
    void eventsAfterTheTransactionCompletedAreHandedOverImmediately() {
        var provider = provider();
        provider.onEvent(userEvent(EventType.LOGIN));
        keycloak.transactions.commit();
        assertThat(published, hasSize(1));

        provider.onEvent(userEvent(EventType.LOGOUT));

        assertThat(published, hasSize(2));
    }

    @Test
    void eventFiredWhileKeycloakRunsAfterCompletionTransactionsIsHandedOverImmediately() {
        var provider = provider();
        // Another after-completion transaction (enlisted before ours) fires an event during the commit loop:
        // the manager is still active but already completed, so enlisting is refused.
        keycloak.transactions.enlistAfterCompletion(new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
                provider.onEvent(userEvent(EventType.LOGOUT));
            }

            @Override
            protected void rollbackImpl() {
            }
        });

        keycloak.transactions.commit();

        assertThat(published, hasSize(1));
        assertThat(failures.count(), equalTo(0L));
    }

    @Test
    void sourceFallsBackToTheUrnWithoutHttpContext() throws IOException {
        // E.g. an event fired from a scheduled task with no fixed hostname URL.
        when(keycloak.context.getUri(UrlType.FRONTEND)).thenThrow(new ContextNotActiveException());

        provider().onEvent(userEvent(EventType.LOGIN));
        keycloak.transactions.commit();

        assertThat(json(published.get(0)).get("source").asText(), equalTo("urn:keycloak:realm:" + REALM_ID));
    }

    @Test
    void sourceUsesTheEventRealmsFrontendUrl() throws IOException {
        when(keycloak.context.getRealm()).thenReturn(null);
        when(keycloak.realm.getAttribute("frontendUrl")).thenReturn("https://sso.example.com");

        provider().onEvent(userEvent(EventType.LOGIN));
        keycloak.transactions.commit();

        assertThat(json(published.get(0)).get("source").asText(),
                equalTo("https://sso.example.com/realms/kafka-demo"));
    }

    @Test
    void realmDeleteIsSourcedFromTheAdminRealm() throws IOException {
        // RealmAdminResource.deleteRealm: the event carries the admin's realm, the context is the deleted realm.
        AdminEvent delete = adminEvent(ResourceType.REALM, OperationType.DELETE, "kafka-demo");
        delete.setRealmId(MASTER_ID);
        delete.setRealmName("master");

        provider().onEvent(delete, false);
        keycloak.transactions.commit();

        ProducerRecord<String, byte[]> record = published.get(0);
        assertThat(json(record).get("source").asText(), equalTo("http://localhost:8080/realms/master"));
        assertThat(json(record).get("kcrealmid").asText(), equalTo(MASTER_ID));
        assertThat(record.topic(), equalTo("keycloak.admin-events"));
        assertThat(record.key(), equalTo(MASTER_ID + ":kafka-demo"));
    }

    @Test
    void adminRepresentationNeedsBothKeycloakAndConfiguration() throws IOException {
        options = new EnvelopeOptions(true, true, DetailRedaction.DEFAULTS);
        var provider = provider();

        provider.onEvent(adminEvent(ResourceType.USER, OperationType.CREATE, "users/1"), true);
        provider.onEvent(adminEvent(ResourceType.USER, OperationType.CREATE, "users/2"), false);
        keycloak.transactions.commit();

        assertThat(json(published.get(0)).get("data").get("representation").get("username").asText(),
                equalTo("new-user"));
        assertThat(json(published.get(1)).get("data").has("representation"), is(false));
    }

    @Test
    void preparationFailureNeverPropagatesAndIsCounted() {
        Event broken = new Event() {
            @Override
            public Map<String, String> getDetails() {
                throw new IllegalStateException("broken extension event");
            }
        };
        broken.setType(EventType.LOGIN);
        broken.setRealmId(REALM_ID);
        AdminEvent brokenAdmin = new AdminEvent() {
            @Override
            public Map<String, String> getDetails() {
                throw new IllegalStateException("broken extension event");
            }
        };
        brokenAdmin.setResourceTypeAsString("USER");
        brokenAdmin.setRealmId(REALM_ID);
        var provider = provider();

        assertDoesNotThrow(() -> provider.onEvent(broken));
        assertDoesNotThrow(() -> provider.onEvent(brokenAdmin, false));
        provider.onEvent(userEvent(EventType.LOGIN));
        keycloak.transactions.commit();

        assertThat(failures.count(), equalTo(2L));
        assertThat("later events still publish", published, hasSize(1));
    }

    @Test
    void sinkFailureAtCommitNeverFailsTheCommit() {
        sink = record -> {
            throw new IllegalStateException("sender gone");
        };
        var provider = provider();
        provider.onEvent(userEvent(EventType.LOGIN));
        provider.onEvent(userEvent(EventType.LOGOUT));

        assertDoesNotThrow(keycloak.transactions::commit);

        assertThat(failures.count(), equalTo(2L));
    }

    private KafkaEventListenerProvider provider() {
        return new KafkaEventListenerProvider(keycloak.session, pipeline());
    }

    private ListenerPipeline pipeline() {
        return new ListenerPipeline(
                filter,
                new EnvelopeMapper(options),
                new EventRecordFactory(new TopicRouter("keycloak.events", "keycloak.admin-events", Map.of()),
                        new KeyStrategy()),
                record -> sink.offer(record),
                failures);
    }

    private static JsonNode json(ProducerRecord<String, byte[]> record) throws IOException {
        return JsonSerialization.readValue(record.value(), JsonNode.class);
    }
}
