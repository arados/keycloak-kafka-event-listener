package hr.delmisoft.keycloak.kafka;

/**
 * Provider id and SPI config keys.
 *
 * <p>Config keys are read from the factory's {@code Config.Scope}, i.e.
 * {@code --spi-events-listener--kafka-events--<key>} on the command line or
 * {@code KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__<KEY>} in the environment.
 */
public final class KafkaEventsConst {

    /** Id an admin selects under Realm settings → Events → Event listeners. */
    public static final String PROVIDER_ID = "kafka-events";

    public static final String CFG_BOOTSTRAP_SERVERS = "bootstrap-servers";
    public static final String CFG_TOPIC_EVENTS = "topic-events";
    public static final String CFG_TOPIC_ADMIN_EVENTS = "topic-admin-events";

    public static final String DEFAULT_TOPIC_EVENTS = "keycloak.events";
    public static final String DEFAULT_TOPIC_ADMIN_EVENTS = "keycloak.admin-events";

    private KafkaEventsConst() {
    }
}
