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
import static hr.delmisoft.keycloak.kafka.config.OptionReader.invalid;

import java.time.Duration;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.keycloak.Config;
import org.keycloak.events.EventType;

/**
 * Validated, immutable listener configuration, read once at startup from the factory's
 * {@link Config.Scope} via {@link #from(Config.Scope)}.
 *
 * <p>Unconfigured = disabled: without {@code bootstrap-servers} {@link #from} returns
 * {@link Optional#empty()} and reads nothing else. Misconfigured = fail fast: any invalid value throws
 * {@link KafkaEventsConfigException} naming the option.
 *
 * @param deliveryMode              {@code delivery-mode}
 * @param topicEvents               {@code topic-events}, a raw topic template (validated by the router)
 * @param topicAdminEvents          {@code topic-admin-events}, a raw topic template
 * @param topicOverrides            {@code topic-overrides}: user event type → raw topic template
 * @param includeEventTypes         {@code include-event-types}; empty = all
 * @param excludeEventTypes         {@code exclude-event-types}
 * @param includeAdminResourceTypes {@code include-admin-resource-types}, upper case; empty = all
 * @param excludeAdminResourceTypes {@code exclude-admin-resource-types}, upper case
 * @param includeIpAddress          {@code include-ip-address}
 * @param includeRepresentation     {@code include-representation}
 * @param allowedDetailKeys         {@code allowed-detail-keys}, lower case
 * @param redactedDetailKeys        {@code redacted-detail-keys}, lower case
 * @param queueCapacity             {@code queue-capacity}
 * @param closeTimeout              {@code close-timeout-ms}
 * @param outbox                    the {@code outbox-*} options (used in outbox mode only)
 * @param producerProperties        complete Kafka producer properties, including {@code bootstrap.servers};
 *                                  may contain secrets, never log them (use {@link #toString()})
 */
public record KafkaEventsConfig(
        DeliveryMode deliveryMode,
        String topicEvents,
        String topicAdminEvents,
        Map<EventType, String> topicOverrides,
        Set<EventType> includeEventTypes,
        Set<EventType> excludeEventTypes,
        Set<String> includeAdminResourceTypes,
        Set<String> excludeAdminResourceTypes,
        boolean includeIpAddress,
        boolean includeRepresentation,
        Set<String> allowedDetailKeys,
        Set<String> redactedDetailKeys,
        int queueCapacity,
        Duration closeTimeout,
        OutboxOptions outbox,
        Map<String, String> producerProperties) {

    /** Smallest {@code outbox-drain-interval-ms}. */
    public static final long MIN_DRAIN_INTERVAL_MS = 1_000;

    public KafkaEventsConfig {
        Objects.requireNonNull(deliveryMode, "deliveryMode");
        Objects.requireNonNull(topicEvents, "topicEvents");
        Objects.requireNonNull(topicAdminEvents, "topicAdminEvents");
        Objects.requireNonNull(closeTimeout, "closeTimeout");
        Objects.requireNonNull(outbox, "outbox");
        topicOverrides = Map.copyOf(topicOverrides);
        includeEventTypes = Set.copyOf(includeEventTypes);
        excludeEventTypes = Set.copyOf(excludeEventTypes);
        includeAdminResourceTypes = Set.copyOf(includeAdminResourceTypes);
        excludeAdminResourceTypes = Set.copyOf(excludeAdminResourceTypes);
        allowedDetailKeys = Set.copyOf(allowedDetailKeys);
        redactedDetailKeys = Set.copyOf(redactedDetailKeys);
        producerProperties = Map.copyOf(producerProperties);
    }

    /**
     * Reads and validates the configuration.
     *
     * @return the configuration, or empty when {@code bootstrap-servers} is missing or blank (listener
     *         disabled; the caller logs one WARN)
     * @throws KafkaEventsConfigException when any option is invalid
     */
    public static Optional<KafkaEventsConfig> from(Config.Scope scope) {
        return from(scope, ProducerProperties::localHostname);
    }

    static Optional<KafkaEventsConfig> from(Config.Scope scope, Supplier<String> hostname) {
        OptionReader options = new OptionReader(scope);
        String bootstrapServers = options.raw(CFG_BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new KafkaEventsConfig(
                deliveryMode(options),
                options.string(CFG_TOPIC_EVENTS, DEFAULT_TOPIC_EVENTS),
                options.string(CFG_TOPIC_ADMIN_EVENTS, DEFAULT_TOPIC_ADMIN_EVENTS),
                topicOverrides(options),
                eventTypes(options, CFG_INCLUDE_EVENT_TYPES),
                eventTypes(options, CFG_EXCLUDE_EVENT_TYPES),
                normalized(options, CFG_INCLUDE_ADMIN_RESOURCE_TYPES, true),
                normalized(options, CFG_EXCLUDE_ADMIN_RESOURCE_TYPES, true),
                options.bool(CFG_INCLUDE_IP_ADDRESS, DEFAULT_INCLUDE_IP_ADDRESS),
                options.bool(CFG_INCLUDE_REPRESENTATION, DEFAULT_INCLUDE_REPRESENTATION),
                normalized(options, CFG_ALLOWED_DETAIL_KEYS, false),
                normalized(options, CFG_REDACTED_DETAIL_KEYS, false),
                options.positiveInt(CFG_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY),
                Duration.ofMillis(options.nonNegativeLong(CFG_CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS)),
                outbox(options),
                ProducerProperties.build(scope, bootstrapServers, hostname)));
    }

    /** The {@code bootstrap-servers} option (also present in {@link #producerProperties()}). */
    public String bootstrapServers() {
        return producerProperties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    private static DeliveryMode deliveryMode(OptionReader options) {
        String value = options.raw(CFG_DELIVERY_MODE);
        if (value == null || value.isEmpty()) {
            return DeliveryMode.ASYNC;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "async" -> DeliveryMode.ASYNC;
            case "outbox" -> DeliveryMode.OUTBOX;
            default -> throw invalid(CFG_DELIVERY_MODE, value, "supported: " + String.join(", ", DeliveryMode.names()));
        };
    }

    private static OutboxOptions outbox(OptionReader options) {
        OutboxOptions defaults = OutboxOptions.DEFAULTS;
        long drainIntervalMs = options.nonNegativeLong(CFG_OUTBOX_DRAIN_INTERVAL_MS,
                defaults.drainInterval().toMillis());
        if (drainIntervalMs < MIN_DRAIN_INTERVAL_MS) {
            // Keycloak's cluster-wide task lock works in whole seconds.
            throw invalid(CFG_OUTBOX_DRAIN_INTERVAL_MS, Long.toString(drainIntervalMs),
                    "must be at least " + MIN_DRAIN_INTERVAL_MS);
        }
        long deadLetterRetentionMs = options.nonNegativeLong(CFG_OUTBOX_DEAD_LETTER_RETENTION_MS,
                defaults.deadLetterRetention().toMillis());
        long pendingMaxAgeMs = options.nonNegativeLong(CFG_OUTBOX_PENDING_MAX_AGE_MS,
                defaults.pendingMaxAge().toMillis());
        if (deadLetterRetentionMs > 0 && pendingMaxAgeMs > 0 && deadLetterRetentionMs <= pendingMaxAgeMs) {
            // Both count from the event's creation: rows dead-lettered for their age would be purged at once.
            throw invalid(CFG_OUTBOX_DEAD_LETTER_RETENTION_MS, Long.toString(deadLetterRetentionMs),
                    "must be greater than " + CFG_OUTBOX_PENDING_MAX_AGE_MS + " (" + pendingMaxAgeMs
                            + "), or 0 to keep dead letters forever");
        }
        return new OutboxOptions(
                Duration.ofMillis(drainIntervalMs),
                options.positiveInt(CFG_OUTBOX_BATCH_SIZE, defaults.batchSize()),
                options.positiveInt(CFG_OUTBOX_MAX_ATTEMPTS, defaults.maxAttempts()),
                Duration.ofMillis(options.nonNegativeLong(CFG_OUTBOX_DELIVERED_RETENTION_MS,
                        defaults.deliveredRetention().toMillis())),
                Duration.ofMillis(deadLetterRetentionMs),
                Duration.ofMillis(pendingMaxAgeMs));
    }

    private static Set<EventType> eventTypes(OptionReader options, String key) {
        Set<EventType> result = EnumSet.noneOf(EventType.class);
        for (String entry : options.list(key)) {
            result.add(eventType(key, entry));
        }
        return result;
    }

    private static EventType eventType(String key, String name) {
        try {
            return EventType.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw invalid(key, name, "not a Keycloak user event type, e.g. LOGIN, LOGIN_ERROR");
        }
    }

    private static Set<String> normalized(OptionReader options, String key, boolean upperCase) {
        Set<String> result = new LinkedHashSet<>();
        for (String entry : options.list(key)) {
            result.add(upperCase ? entry.toUpperCase(Locale.ROOT) : entry.toLowerCase(Locale.ROOT));
        }
        return result;
    }

    private static Map<EventType, String> topicOverrides(OptionReader options) {
        Map<EventType, String> result = new EnumMap<>(EventType.class);
        for (String entry : options.list(CFG_TOPIC_OVERRIDES)) {
            int separator = entry.indexOf('=');
            String type = separator < 0 ? "" : entry.substring(0, separator).trim();
            String topic = separator < 0 ? "" : entry.substring(separator + 1).trim();
            if (type.isEmpty() || topic.isEmpty()) {
                throw invalid(CFG_TOPIC_OVERRIDES, entry, "entries must be EVENT_TYPE=topic");
            }
            EventType eventType = eventType(CFG_TOPIC_OVERRIDES, type);
            if (result.put(eventType, topic) != null) {
                throw invalid(CFG_TOPIC_OVERRIDES, entry, "duplicate override for " + eventType);
            }
        }
        return result;
    }

    /** Safe to log: values of sensitive producer properties are masked. */
    @Override
    public String toString() {
        StringJoiner producer = new StringJoiner(", ", "{", "}");
        new TreeMap<>(producerProperties).forEach(
                (key, value) -> producer.add(key + "=" + ProducerProperties.display(key, value)));
        return "KafkaEventsConfig["
                + "deliveryMode=" + deliveryMode
                + ", topicEvents=" + topicEvents
                + ", topicAdminEvents=" + topicAdminEvents
                + ", topicOverrides=" + new TreeMap<>(topicOverrides)
                + ", includeEventTypes=" + new TreeSet<>(includeEventTypes)
                + ", excludeEventTypes=" + new TreeSet<>(excludeEventTypes)
                + ", includeAdminResourceTypes=" + new TreeSet<>(includeAdminResourceTypes)
                + ", excludeAdminResourceTypes=" + new TreeSet<>(excludeAdminResourceTypes)
                + ", includeIpAddress=" + includeIpAddress
                + ", includeRepresentation=" + includeRepresentation
                + ", allowedDetailKeys=" + new TreeSet<>(allowedDetailKeys)
                + ", redactedDetailKeys=" + new TreeSet<>(redactedDetailKeys)
                + ", queueCapacity=" + queueCapacity
                + ", closeTimeout=" + closeTimeout
                + ", outbox=" + outbox
                + ", producerProperties=" + producer
                + "]";
    }
}
