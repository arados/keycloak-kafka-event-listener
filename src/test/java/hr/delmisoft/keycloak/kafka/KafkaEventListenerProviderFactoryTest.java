package hr.delmisoft.keycloak.kafka;

import static hr.delmisoft.keycloak.kafka.KeycloakFixture.REALM_ID;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.USER_ID;
import static hr.delmisoft.keycloak.kafka.KeycloakFixture.userEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfigException;
import hr.delmisoft.keycloak.kafka.metrics.MicrometerEventMetrics;
import hr.delmisoft.keycloak.kafka.sender.ProducerCreationException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.ProviderConfigProperty;

class KafkaEventListenerProviderFactoryTest {

    private final Map<String, String> options = new HashMap<>();
    private final MockProducer<String, byte[]> producer =
            new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    private final List<Map<String, String>> producerProperties = new ArrayList<>();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final KafkaEventListenerProviderFactory factory = new KafkaEventListenerProviderFactory(properties -> {
        producerProperties.add(properties);
        return producer;
    }, () -> new MicrometerEventMetrics(registry));

    @AfterEach
    void closeFactory() {
        factory.close();
    }

    @Test
    void providerIdIsStable() {
        // Realms reference the listener by this id; changing it silently disables publishing.
        assertThat(factory.getId(), equalTo("kafka-events"));
    }

    @Test
    void unconfiguredListenerIsDisabledAndCreatesNothing() {
        factory.init(scope());
        factory.postInit(mock(KeycloakSessionFactory.class));
        KeycloakSession session = mock(KeycloakSession.class);

        EventListenerProvider provider = factory.create(session);
        provider.onEvent(userEvent(EventType.LOGIN));
        provider.onEvent(new AdminEvent(), true);
        provider.close();

        assertThat(provider, not(instanceOf(KafkaEventListenerProvider.class)));
        assertThat("no producer is created", producerProperties, empty());
        assertThat(factory.sender(), nullValue());
        verifyNoInteractions(session);
    }

    @Test
    void invalidConfigurationFailsStartupNamingTheOption() {
        options.put("bootstrap-servers", "kafka:9092");
        options.put("topic-events", "events/{realm}");

        var error = assertThrows(KafkaEventsConfigException.class, () -> factory.init(scope()));

        assertThat(error.getMessage(), containsString("spi-events-listener--kafka-events--topic-events"));
    }

    @Test
    void unknownProducerOptionFailsStartup() {
        options.put("bootstrap-servers", "kafka:9092");
        options.put("producer-lingerr-ms", "5");

        var error = assertThrows(KafkaEventsConfigException.class, () -> factory.init(scope()));

        assertThat(error.getMessage(), containsString("spi-events-listener--kafka-events--producer-lingerr-ms"));
    }

    @Test
    void producerCreationFailureFailsStartup() {
        options.put("bootstrap-servers", "kafka:9092");
        var failing = new KafkaEventListenerProviderFactory(properties -> {
            throw new ProducerCreationException("Cannot create the Kafka producer", new IllegalStateException());
        }, () -> new MicrometerEventMetrics(registry));
        failing.init(scope());

        assertThrows(ProducerCreationException.class, () -> failing.postInit(mock(KeycloakSessionFactory.class)));
        assertThat(failing.create(mock(KeycloakSession.class)), not(instanceOf(KafkaEventListenerProvider.class)));
    }

    @Test
    void configuredListenerPublishesCommittedEventsThroughTheProducer() {
        options.put("bootstrap-servers", "kafka:9092");
        options.put("topic-events", "kc.{realmName}.events");
        options.put("producer-linger-ms", "5");
        factory.init(scope());
        factory.postInit(mock(KeycloakSessionFactory.class));
        KeycloakFixture keycloak = new KeycloakFixture().begin();

        EventListenerProvider provider = factory.create(keycloak.session);
        provider.onEvent(userEvent(EventType.LOGIN));
        keycloak.transactions.commit();
        // close() drains the sender's queue into the producer and closes it.
        factory.close();

        assertThat(producerProperties, hasSize(1));
        assertThat(producerProperties.get(0), hasEntry("bootstrap.servers", "kafka:9092"));
        assertThat(producerProperties.get(0), hasEntry("linger.ms", "5"));
        assertThat(producer.history(), hasSize(1));
        assertThat(producer.history().get(0).topic(), equalTo("kc.kafka-demo.events"));
        assertThat(producer.history().get(0).key(), equalTo(REALM_ID + ":" + USER_ID));
        assertThat(producer.closed(), is(true));
    }

    @Test
    void metricsAreRegisteredWhileRunningAndRemovedOnClose() {
        options.put("bootstrap-servers", "kafka:9092");
        factory.init(scope());
        factory.postInit(mock(KeycloakSessionFactory.class));
        KeycloakFixture keycloak = new KeycloakFixture().begin();
        factory.create(keycloak.session).onEvent(userEvent(EventType.LOGIN));
        keycloak.transactions.commit();

        assertThat(registry.get("keycloak.kafka.events.enqueued").tag("kind", "user").counter().count(),
                equalTo(1.0));
        assertThat(registry.get("keycloak.kafka.events.queue.capacity").gauge().value(), equalTo(10_000.0));

        factory.close();
        assertThat(registry.getMeters(), empty());
    }

    @Test
    void configMetadataDeclaresEveryFixedOption() {
        List<String> declared = factory.getConfigMetadata().stream().map(ProviderConfigProperty::getName).toList();

        assertThat(declared, containsInAnyOrder(KafkaEventsConst.OPTIONS.toArray()));
    }

    @Test
    void closeClosesTheSenderAndIsIdempotent() {
        options.put("bootstrap-servers", "kafka:9092");
        factory.init(scope());
        factory.postInit(mock(KeycloakSessionFactory.class));
        var sender = factory.sender();

        factory.close();
        factory.close();

        assertThat(producer.closed(), is(true));
        assertThat("offers after close are refused", sender.offer(
                new org.apache.kafka.clients.producer.ProducerRecord<>("t", new byte[0])), is(false));
        assertThat(factory.create(mock(KeycloakSession.class)), not(instanceOf(KafkaEventListenerProvider.class)));
    }

    @Test
    void envNameUsesTheDoubleUnderscoreForm() {
        assertThat(KafkaEventListenerProviderFactory.envName("bootstrap-servers"),
                equalTo("KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS"));
    }

    /**
     * Mocks the Quarkus-backed scope: {@code get} takes the option suffix, {@code getPropertyNames} returns
     * full {@code kc.spi-events-listener--kafka-events--*} keys, as Keycloak 26.8.1 does.
     */
    private Config.Scope scope() {
        Config.Scope scope = mock(Config.Scope.class);
        when(scope.get(anyString())).thenAnswer(invocation -> options.get(invocation.<String>getArgument(0)));
        when(scope.getPropertyNames()).thenAnswer(invocation -> options.keySet().stream()
                .map(key -> KafkaEventsConst.PROPERTY_NAME_PREFIX + key)
                .collect(Collectors.toSet()));
        return scope;
    }
}
