package hr.delmisoft.keycloak.kafka;

import java.util.List;

/**
 * Provider id and SPI config keys.
 *
 * <p>Config keys are read from the factory's {@code Config.Scope}, i.e.
 * {@code --spi-events-listener--kafka-events--<key>} on the command line,
 * {@code KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__<KEY>} in the environment or
 * {@code spi-events-listener--kafka-events--<key>} in {@code keycloak.conf}.
 *
 * <p>No option name may end in {@code -enabled}, {@code -provider} or {@code -provider-default}:
 * Keycloak treats such SPI keys as build-time options and refuses {@code start --optimized} when they
 * are set at runtime (docs/RESEARCH.md §6.1).
 */
public final class KafkaEventsConst {

    /** Id an admin selects under Realm settings → Events → Event listeners. */
    public static final String PROVIDER_ID = "kafka-events";

    /** Option name prefix as an operator types it (CLI without {@code --}, keycloak.conf). */
    public static final String OPTION_PREFIX = "spi-events-listener--" + PROVIDER_ID + "--";

    /** Prefix of the full keys {@code Config.Scope#getPropertyNames()} returns under Quarkus. */
    public static final String PROPERTY_NAME_PREFIX = "kc." + OPTION_PREFIX;

    public static final String CFG_BOOTSTRAP_SERVERS = "bootstrap-servers";
    public static final String CFG_TOPIC_EVENTS = "topic-events";
    public static final String CFG_TOPIC_ADMIN_EVENTS = "topic-admin-events";
    public static final String CFG_TOPIC_OVERRIDES = "topic-overrides";
    public static final String CFG_INCLUDE_EVENT_TYPES = "include-event-types";
    public static final String CFG_EXCLUDE_EVENT_TYPES = "exclude-event-types";
    public static final String CFG_INCLUDE_ADMIN_RESOURCE_TYPES = "include-admin-resource-types";
    public static final String CFG_EXCLUDE_ADMIN_RESOURCE_TYPES = "exclude-admin-resource-types";
    public static final String CFG_INCLUDE_IP_ADDRESS = "include-ip-address";
    public static final String CFG_INCLUDE_REPRESENTATION = "include-representation";
    public static final String CFG_ALLOWED_DETAIL_KEYS = "allowed-detail-keys";
    public static final String CFG_REDACTED_DETAIL_KEYS = "redacted-detail-keys";
    public static final String CFG_DELIVERY_MODE = "delivery-mode";
    public static final String CFG_QUEUE_CAPACITY = "queue-capacity";
    public static final String CFG_CLOSE_TIMEOUT_MS = "close-timeout-ms";
    public static final String CFG_PRODUCER_PROPERTIES_FILE = "producer-properties-file";

    /**
     * Prefix of the Kafka producer passthrough options: {@code producer-linger-ms} → {@code linger.ms}.
     * {@link #CFG_PRODUCER_PROPERTIES_FILE} shares the prefix but is not a passthrough option.
     */
    public static final String CFG_PRODUCER_PREFIX = "producer-";

    /** Every fixed option name (the {@code producer-*} passthrough family is open-ended). */
    public static final List<String> OPTIONS = List.of(
            CFG_BOOTSTRAP_SERVERS,
            CFG_TOPIC_EVENTS,
            CFG_TOPIC_ADMIN_EVENTS,
            CFG_TOPIC_OVERRIDES,
            CFG_INCLUDE_EVENT_TYPES,
            CFG_EXCLUDE_EVENT_TYPES,
            CFG_INCLUDE_ADMIN_RESOURCE_TYPES,
            CFG_EXCLUDE_ADMIN_RESOURCE_TYPES,
            CFG_INCLUDE_IP_ADDRESS,
            CFG_INCLUDE_REPRESENTATION,
            CFG_ALLOWED_DETAIL_KEYS,
            CFG_REDACTED_DETAIL_KEYS,
            CFG_DELIVERY_MODE,
            CFG_QUEUE_CAPACITY,
            CFG_CLOSE_TIMEOUT_MS,
            CFG_PRODUCER_PROPERTIES_FILE);

    public static final String DEFAULT_TOPIC_EVENTS = "keycloak.events";
    public static final String DEFAULT_TOPIC_ADMIN_EVENTS = "keycloak.admin-events";
    public static final boolean DEFAULT_INCLUDE_IP_ADDRESS = true;
    public static final boolean DEFAULT_INCLUDE_REPRESENTATION = false;
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    public static final long DEFAULT_CLOSE_TIMEOUT_MS = 5_000;

    private KafkaEventsConst() {
    }
}
