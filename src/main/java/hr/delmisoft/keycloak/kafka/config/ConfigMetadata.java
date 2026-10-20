package hr.delmisoft.keycloak.kafka.config;

import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_ALLOWED_DETAIL_KEYS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_BOOTSTRAP_SERVERS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_CLOSE_TIMEOUT_MS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_DELIVERY_MODE;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_EXCLUDE_ADMIN_RESOURCE_TYPES;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_EXCLUDE_EVENT_TYPES;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_INCLUDE_ADMIN_RESOURCE_TYPES;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_INCLUDE_EVENT_TYPES;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_INCLUDE_IP_ADDRESS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_INCLUDE_REPRESENTATION;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_OUTBOX_BATCH_SIZE;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_OUTBOX_DEAD_LETTER_RETENTION_MS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_OUTBOX_DELIVERED_RETENTION_MS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_OUTBOX_DRAIN_INTERVAL_MS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_OUTBOX_MAX_ATTEMPTS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_OUTBOX_PENDING_MAX_AGE_MS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_PRODUCER_PROPERTIES_FILE;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_QUEUE_CAPACITY;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_REDACTED_DETAIL_KEYS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_TOPIC_ADMIN_EVENTS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_TOPIC_EVENTS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_TOPIC_OVERRIDES;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.DEFAULT_CLOSE_TIMEOUT_MS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.DEFAULT_INCLUDE_IP_ADDRESS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.DEFAULT_INCLUDE_REPRESENTATION;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.DEFAULT_QUEUE_CAPACITY;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.DEFAULT_TOPIC_ADMIN_EVENTS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.DEFAULT_TOPIC_EVENTS;
import static org.keycloak.provider.ProviderConfigProperty.BOOLEAN_TYPE;
import static org.keycloak.provider.ProviderConfigProperty.INTEGER_TYPE;
import static org.keycloak.provider.ProviderConfigProperty.LIST_TYPE;
import static org.keycloak.provider.ProviderConfigProperty.STRING_TYPE;

import java.util.List;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;

/**
 * Declares the fixed options for {@code ProviderFactory#getConfigMetadata()}. Keycloak 26.8.1 neither validates
 * SPI options against this nor displays it for event listeners; it documents the options in code and lets
 * future Keycloak versions surface them. The open-ended {@code producer-*} passthrough family is not listed.
 * README "Configuration" is the user-facing reference; keep the two in sync.
 */
public final class ConfigMetadata {

    private ConfigMetadata() {
    }

    public static List<ProviderConfigProperty> options() {
        OutboxOptions defaults = OutboxOptions.DEFAULTS;
        return ProviderConfigurationBuilder.create()
                .property().name(CFG_BOOTSTRAP_SERVERS).type(STRING_TYPE)
                .helpText("Kafka bootstrap servers, host:port[,host:port]. Unset = listener disabled.").add()
                .property().name(CFG_TOPIC_EVENTS).type(STRING_TYPE).defaultValue(DEFAULT_TOPIC_EVENTS)
                .helpText("Topic for user events; may contain {realmId} and {realmName}.").add()
                .property().name(CFG_TOPIC_ADMIN_EVENTS).type(STRING_TYPE).defaultValue(DEFAULT_TOPIC_ADMIN_EVENTS)
                .helpText("Topic for admin events; may contain {realmId} and {realmName}.").add()
                .property().name(CFG_TOPIC_OVERRIDES).type(STRING_TYPE)
                .helpText("Comma list of EVENT_TYPE=topic, e.g. LOGIN_ERROR=keycloak.security.").add()
                .property().name(CFG_INCLUDE_EVENT_TYPES).type(STRING_TYPE)
                .helpText("Comma list of user event types to publish; empty = all.").add()
                .property().name(CFG_EXCLUDE_EVENT_TYPES).type(STRING_TYPE)
                .helpText("Comma list of user event types not to publish; wins over the include list.").add()
                .property().name(CFG_INCLUDE_ADMIN_RESOURCE_TYPES).type(STRING_TYPE)
                .helpText("Comma list of admin resource types to publish, e.g. USER,GROUP; empty = all.").add()
                .property().name(CFG_EXCLUDE_ADMIN_RESOURCE_TYPES).type(STRING_TYPE)
                .helpText("Comma list of admin resource types not to publish; wins over the include list.").add()
                .property().name(CFG_INCLUDE_IP_ADDRESS).type(BOOLEAN_TYPE).defaultValue(DEFAULT_INCLUDE_IP_ADDRESS)
                .helpText("Include the client IP address in user events and admin authDetails.").add()
                .property().name(CFG_INCLUDE_REPRESENTATION).type(BOOLEAN_TYPE)
                .defaultValue(DEFAULT_INCLUDE_REPRESENTATION)
                .helpText("Include admin event representations when the realm's 'Include representation' is on.")
                .add()
                .property().name(CFG_ALLOWED_DETAIL_KEYS).type(STRING_TYPE)
                .helpText("Comma list of extra event details keys published unredacted.").add()
                .property().name(CFG_REDACTED_DETAIL_KEYS).type(STRING_TYPE)
                .helpText("Comma list of extra event details keys that are always redacted.").add()
                .property().name(CFG_DELIVERY_MODE).type(LIST_TYPE).options(DeliveryMode.names())
                .defaultValue(DeliveryMode.ASYNC.optionValue())
                .helpText("async: after commit via an in-memory queue (at-most-once); outbox: through Keycloak's"
                        + " OUTBOX_ENTRY table (at-least-once).").add()
                .property().name(CFG_QUEUE_CAPACITY).type(INTEGER_TYPE).defaultValue(DEFAULT_QUEUE_CAPACITY)
                .helpText("Async mode: events buffered in memory; a full queue drops events.").add()
                .property().name(CFG_CLOSE_TIMEOUT_MS).type(INTEGER_TYPE).defaultValue(DEFAULT_CLOSE_TIMEOUT_MS)
                .helpText("Shutdown budget for sending queued events, in milliseconds.").add()
                .property().name(CFG_PRODUCER_PROPERTIES_FILE).type(STRING_TYPE)
                .helpText("Path to a .properties file with Kafka producer settings (TLS/SASL secrets).").add()
                .property().name(CFG_OUTBOX_DRAIN_INTERVAL_MS).type(INTEGER_TYPE)
                .defaultValue(defaults.drainInterval().toMillis())
                .helpText("Outbox mode: how often the outbox is drained, in milliseconds (at least 1000).").add()
                .property().name(CFG_OUTBOX_BATCH_SIZE).type(INTEGER_TYPE).defaultValue(defaults.batchSize())
                .helpText("Outbox mode: events sent per drain run.").add()
                .property().name(CFG_OUTBOX_MAX_ATTEMPTS).type(INTEGER_TYPE).defaultValue(defaults.maxAttempts())
                .helpText("Outbox mode: attempts for events Kafka rejects before they are dead-lettered.").add()
                .property().name(CFG_OUTBOX_DELIVERED_RETENTION_MS).type(INTEGER_TYPE)
                .defaultValue(defaults.deliveredRetention().toMillis())
                .helpText("Outbox mode: how long delivered rows are kept, in milliseconds; 0 deletes them.").add()
                .property().name(CFG_OUTBOX_DEAD_LETTER_RETENTION_MS).type(INTEGER_TYPE)
                .defaultValue(defaults.deadLetterRetention().toMillis())
                .helpText("Outbox mode: how long dead-lettered rows are kept, in milliseconds; 0 = forever.").add()
                .property().name(CFG_OUTBOX_PENDING_MAX_AGE_MS).type(INTEGER_TYPE)
                .defaultValue(defaults.pendingMaxAge().toMillis())
                .helpText("Outbox mode: undelivered rows older than this are dead-lettered; 0 = never.").add()
                .build();
    }
}
