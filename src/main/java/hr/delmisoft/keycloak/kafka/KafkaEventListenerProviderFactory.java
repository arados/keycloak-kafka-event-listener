package hr.delmisoft.keycloak.kafka;

import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_BOOTSTRAP_SERVERS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.OPTION_PREFIX;

import hr.delmisoft.keycloak.kafka.config.ConfigMetadata;
import hr.delmisoft.keycloak.kafka.config.DeliveryMode;
import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import hr.delmisoft.keycloak.kafka.envelope.DetailRedaction;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeOptions;
import hr.delmisoft.keycloak.kafka.metrics.MicrometerEventMetrics;
import hr.delmisoft.keycloak.kafka.outbox.KafkaOutbox;
import hr.delmisoft.keycloak.kafka.outbox.OutboxRuntime;
import hr.delmisoft.keycloak.kafka.routing.EventFilter;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import hr.delmisoft.keycloak.kafka.sender.KafkaProducers;
import hr.delmisoft.keycloak.kafka.sender.KafkaSender;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.ProviderConfigProperty;

/**
 * Registers the {@value KafkaEventsConst#PROVIDER_ID} event listener and owns the server-wide parts: the
 * configuration, the event filter, mapper and record factory, and the {@link KafkaSender} with its producer.
 *
 * <ul>
 *   <li>{@link #init}: reads and validates the configuration. Unconfigured ({@code bootstrap-servers} unset) =
 *       disabled: one WARN, and {@link #create} hands out a no-op provider. Misconfigured = fail fast: the
 *       {@code KafkaEventsConfigException} (naming the option) propagates and Keycloak does not start.</li>
 *   <li>{@link #postInit}: creates the producer (this does not contact the broker, so Kafka being down never
 *       fails startup; a producer Kafka refuses to construct is a configuration error and does), registers the
 *       metrics, and starts the sender thread (async mode) or schedules the outbox drainer (outbox mode).</li>
 *   <li>{@link #close}: async mode stops accepting and drains the queue for at most {@code close-timeout-ms};
 *       outbox mode cancels the drainer (undelivered rows stay in the outbox). Then closes the producer and
 *       removes the metrics. Idempotent.</li>
 * </ul>
 */
public final class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);

    /** Handed out while the listener is disabled (unconfigured, or not started yet / already closed). */
    private static final EventListenerProvider DISABLED = new DisabledProvider();

    private final Function<Map<String, String>, Producer<String, byte[]>> producerFactory;
    private final Supplier<MicrometerEventMetrics> metricsFactory;
    private final OutboxStarter outboxStarter;

    private KafkaEventsConfig config;
    private EventFilter filter;
    private EnvelopeMapper mapper;
    private EventRecordFactory recordFactory;
    private KafkaSender sender;
    private OutboxRuntime outbox;
    private MicrometerEventMetrics metrics;
    private ListenerFailures failures;

    /** Read by request threads in {@link #create}; {@code null} = disabled. */
    private volatile ListenerPipeline pipeline;

    /** Starts {@link KafkaOutbox}; replaced in tests, which have no Keycloak timer or database. */
    @FunctionalInterface
    interface OutboxStarter {
        OutboxRuntime start(KeycloakSessionFactory sessionFactory, Producer<String, byte[]> producer,
                KafkaEventsConfig config, MicrometerEventMetrics metrics);
    }

    public KafkaEventListenerProviderFactory() {
        this(KafkaProducers::create, MicrometerEventMetrics::new, KafkaEventListenerProviderFactory::startOutbox);
    }

    /**
     * For tests: {@code producerFactory} replaces {@link KafkaProducers#create} (e.g. with a MockProducer),
     * {@code metricsFactory} the global Micrometer registry, {@code outboxStarter} the outbox scheduling.
     */
    KafkaEventListenerProviderFactory(Function<Map<String, String>, Producer<String, byte[]>> producerFactory,
            Supplier<MicrometerEventMetrics> metricsFactory, OutboxStarter outboxStarter) {
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.metricsFactory = Objects.requireNonNull(metricsFactory, "metricsFactory");
        this.outboxStarter = Objects.requireNonNull(outboxStarter, "outboxStarter");
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
                + " (admin events), delivery mode %s", KafkaEventsConst.PROVIDER_ID,
                parsedConfig.bootstrapServers(), parsedConfig.topicEvents(), parsedConfig.topicAdminEvents(),
                parsedConfig.deliveryMode().optionValue());
        // toString() masks sensitive producer settings.
        LOG.debugf("Kafka event listener configuration: %s", parsedConfig);
    }

    @Override
    public void postInit(KeycloakSessionFactory sessionFactory) {
        if (config == null || metrics != null) {
            return;
        }
        // Throws ProducerCreationException (no setting values in the message) when Kafka rejects the settings or
        // a codec JAR is missing: Keycloak then refuses to start.
        Producer<String, byte[]> producer = producerFactory.apply(config.producerProperties());
        metrics = metricsFactory.get();
        failures = new ListenerFailures(metrics);
        if (config.deliveryMode() == DeliveryMode.OUTBOX) {
            outbox = outboxStarter.start(sessionFactory, producer, config, metrics);
            pipeline = new ListenerPipeline(filter, mapper, recordFactory, null, outbox.writer(), failures, metrics);
        } else {
            sender = KafkaSender.start(producer, config.queueCapacity(), config.closeTimeout(), metrics);
            pipeline = new ListenerPipeline(filter, mapper, recordFactory, sender::offer, null, failures, metrics);
        }
    }

    private static OutboxRuntime startOutbox(KeycloakSessionFactory sessionFactory, Producer<String, byte[]> producer,
            KafkaEventsConfig config, MicrometerEventMetrics metrics) {
        Duration deliveryTimeout = Duration.ofMillis(
                Long.parseLong(config.producerProperties().get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG).trim()));
        return KafkaOutbox.start(sessionFactory, producer, config.outbox(), deliveryTimeout, config.closeTimeout(),
                metrics);
    }

    @Override
    public void close() {
        pipeline = null;
        if (metrics == null) {
            return;
        }
        if (sender != null) {
            sender.close();
            sender = null;
        }
        if (outbox != null) {
            outbox.close();
            outbox = null;
        }
        metrics.close();
        metrics = null;
        long failed = failures.count();
        if (failed > 0) {
            LOG.infof("Kafka event listener: %d events could not be prepared for publishing since startup", failed);
        }
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ConfigMetadata.options();
    }

    @Override
    public String getId() {
        return KafkaEventsConst.PROVIDER_ID;
    }

    /** For tests. */
    KafkaSender sender() {
        return sender;
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
