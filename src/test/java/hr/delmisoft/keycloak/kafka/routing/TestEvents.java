package hr.delmisoft.keycloak.kafka.routing;

import hr.delmisoft.keycloak.kafka.config.DeliveryMode;
import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeContext;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeOptions;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

/** Keycloak events and envelopes built through the real {@link EnvelopeMapper}. */
final class TestEvents {

    static final String REALM_ID = "6b1e8f4a-3c2d-4e5f-8a9b-0c1d2e3f4a5b";
    static final String REALM_NAME = "kafka-demo";
    static final String USER_ID = "f3e2d1c0-b9a8-4776-8554-433221100fed";
    static final String SESSION_ID = "9d8c7b6a-5f4e-4d3c-8b2a-1f0e9d8c7b6a";
    static final String ENTITY_ID = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d";

    private static final EnvelopeMapper MAPPER = new EnvelopeMapper(EnvelopeOptions.DEFAULTS);

    private TestEvents() {
    }

    static Event userEvent(EventType type) {
        Event event = new Event();
        event.setId("0b6c1e2a-5d4f-4c3b-9a8e-7f6d5c4b3a21");
        event.setType(type);
        event.setTime(1_760_877_296_789L);
        event.setRealmId(REALM_ID);
        event.setRealmName(REALM_NAME);
        event.setClientId("kafka-demo-client");
        event.setUserId(USER_ID);
        event.setSessionId(SESSION_ID);
        return event;
    }

    static AdminEvent adminEvent(ResourceType type, String resourcePath) {
        AdminEvent event = new AdminEvent();
        event.setId("3e9f4b5d-8a7c-4f6e-ad1b-ac9f8e7d6c54");
        event.setTime(1_760_877_601_234L);
        event.setRealmId(REALM_ID);
        event.setRealmName(REALM_NAME);
        event.setOperationType(OperationType.UPDATE);
        event.setResourceType(type);
        event.setResourcePath(resourcePath);
        return event;
    }

    static Envelope envelope(Event event) {
        return MAPPER.map(event, new EnvelopeContext("urn:keycloak:realm:" + event.getRealmId(), null));
    }

    static Envelope envelope(AdminEvent event) {
        return MAPPER.map(event, false, new EnvelopeContext("urn:keycloak:realm:" + event.getRealmId(), null));
    }

    /** A LOGIN envelope for the given realm (name may be {@code null}). */
    static Envelope login(String realmId, String realmName) {
        Event event = userEvent(EventType.LOGIN);
        event.setRealmId(realmId);
        event.setRealmName(realmName);
        return envelope(event);
    }

    /** A real configuration with the given routing/filter options and defaults for everything else. */
    static KafkaEventsConfig config(
            String topicEvents,
            String topicAdminEvents,
            Map<EventType, String> topicOverrides,
            Set<EventType> includeEventTypes,
            Set<EventType> excludeEventTypes,
            Set<String> includeAdminResourceTypes,
            Set<String> excludeAdminResourceTypes) {
        return new KafkaEventsConfig(
                DeliveryMode.ASYNC,
                topicEvents,
                topicAdminEvents,
                topicOverrides,
                includeEventTypes,
                excludeEventTypes,
                includeAdminResourceTypes,
                excludeAdminResourceTypes,
                true,
                false,
                Set.of(),
                Set.of(),
                10_000,
                Duration.ofSeconds(5),
                Map.of("bootstrap.servers", "localhost:9092"));
    }
}
