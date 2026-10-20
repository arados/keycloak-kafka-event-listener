package hr.delmisoft.keycloak.kafka;

import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_BOOTSTRAP_SERVERS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.OPTION_PREFIX;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import hr.delmisoft.keycloak.kafka.envelope.DetailRedaction;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeOptions;
import hr.delmisoft.keycloak.kafka.routing.EventFilter;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import hr.delmisoft.keycloak.kafka.sender.KafkaProducers;
import hr.delmisoft.keycloak.kafka.sender.KafkaSender;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

/**
 * Registers the {@value KafkaEventsConst#PROVIDER_ID} event listener and owns the server-wide parts: the
 * configuration, the event filter, mapper and record factory, and the {@link KafkaSender} with its producer.
 *
 * <ul>
 *   <li>{@link #init}: reads and validates the configuration. Unconfigured ({@code bootstrap-servers} unset) =
 *       disabled: one WARN, and {@link #create} hands out a no-op provider. Misconfigured = fail fast: the
 *       {@code KafkaEventsConfigException} (naming the option) propagates and Keycloak does not start.</li>
 *   <li>{@link #postInit}: creates the producer (this does not contact the broker, so Kafka being down never
 *       fails startup; a producer Kafka refuses to construct is a configuration error and does) and starts the
 *       sender thread.</li>
 *   <li>{@link #close}: stops accepting, drains the queue for at most {@code close-timeout-ms}, closes the
 *       producer. Idempotent.</li>
 * </ul>
 */
public final class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);

    /** Handed out while the listener is disabled (unconfigured, or not started yet / already closed). */
    private static final EventListenerProvider DISABLED = new DisabledProvider();

    private final Function<Map<String, String>, Producer<String, byte[]>> producerFactory;
    private final ListenerFailures failures = new ListenerFailures();

    private KafkaEventsConfig config;
    private EventFilter filter;
    private EnvelopeMapper mapper;
    private EventRecordFactory recordFactory;
    private KafkaSender sender;

    /** Read by request threads in {@link #create}; {@code null} = disabled. */
    private volatile ListenerPipeline pipeline;

    public KafkaEventListenerProviderFactory() {
        this(KafkaProducers::create);
    }

    /** For tests: {@code producerFactory} replaces {@link KafkaProducers#create} (e.g. with a MockProducer). */
    KafkaEventListenerProviderFactory(Function<Map<String, String>, Producer<String, byte[]>> producerFactory) {
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
    }

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        ListenerPipeline current = pipeline;
        return current == null ? DISABLED : new KafkaEventListenerProvider(session, current);
    }

    @Override
    public void init(Config.Scope scope) {
        Optional<KafkaEventsConfig> parsed = KafkaEventsConfig.from(scope);
        if (parsed.isEmpty()) {
            LOG.warnf("Kafka event listener '%s' is disabled: %s%s is not set (environment variable %s). Realms"
                    + " that enable this listener publish nothing until it is configured", KafkaEventsConst.PROVIDER_ID,
                    OPTION_PREFIX, CFG_BOOTSTRAP_SERVERS, envName(CFG_BOOTSTRAP_SERVERS));
            return;
        }
        KafkaEventsConfig parsedConfig = parsed.get();
        // Everything that can reject the configuration runs now, so a bad value stops startup naming the option.
        filter = EventFilter.from(parsedConfig);
        recordFactory = EventRecordFactory.from(parsedConfig);
        mapper = new EnvelopeMapper(new EnvelopeOptions(
                parsedConfig.includeIpAddress(),
                parsedConfig.includeRepresentation(),
                new DetailRedaction(parsedConfig.allowedDetailKeys(), parsedConfig.redactedDetailKeys())));
        config = parsedConfig;
        LOG.infof("Kafka event listener '%s' configured: bootstrap servers %s, topics '%s' (user events) and '%s'"
                + " (admin events), delivery mode %s, queue capacity %d", KafkaEventsConst.PROVIDER_ID,
                parsedConfig.bootstrapServers(), parsedConfig.topicEvents(), parsedConfig.topicAdminEvents(),
                parsedConfig.deliveryMode().name().toLowerCase(Locale.ROOT), parsedConfig.queueCapacity());
        // toString() masks sensitive producer settings.
        LOG.debugf("Kafka event listener configuration: %s", parsedConfig);
    }

    @Override
    public void postInit(KeycloakSessionFactory sessionFactory) {
        if (config == null || sender != null) {
            return;
        }
        // Throws ProducerCreationException (no setting values in the message) when Kafka rejects the settings or
        // a codec JAR is missing: Keycloak then refuses to start.
        Producer<String, byte[]> producer = producerFactory.apply(config.producerProperties());
        sender = KafkaSender.start(producer, config.queueCapacity(), config.closeTimeout());
        pipeline = new ListenerPipeline(filter, mapper, recordFactory, sender::offer, failures);
    }

    @Override
    public void close() {
        pipeline = null;
        KafkaSender current = sender;
        sender = null;
        if (current == null) {
            return;
        }
        current.close();
        long failed = failures.count();
        if (failed > 0) {
            LOG.infof("Kafka event listener: %d events could not be prepared for publishing since startup", failed);
        }
    }

    @Override
    public String getId() {
        return KafkaEventsConst.PROVIDER_ID;
    }

    /** For tests. */
    KafkaSender sender() {
        return sender;
    }

    /** For tests. */
    ListenerFailures failures() {
        return failures;
    }

    /** {@code bootstrap-servers} → {@code KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS}. */
    static String envName(String key) {
        return "KC_" + (OPTION_PREFIX + key).toUpperCase(Locale.ROOT).replace('-', '_');
    }

    /** Ignores everything. */
    private static final class DisabledProvider implements EventListenerProvider {

        @Override
        public void onEvent(Event event) {
            // disabled
        }

        @Override
        public void onEvent(AdminEvent event, boolean includeRepresentation) {
            // disabled
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
