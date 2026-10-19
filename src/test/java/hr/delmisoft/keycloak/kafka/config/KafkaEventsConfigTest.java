package hr.delmisoft.keycloak.kafka.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import hr.delmisoft.keycloak.kafka.KafkaEventsConst;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.Config;
import org.keycloak.events.EventType;

class KafkaEventsConfigTest {

    private static final String PREFIX = "spi-events-listener--kafka-events--";
    private static final Supplier<String> HOST = () -> "kc-1";

    @TempDir
    Path tempDir;

    private final Map<String, String> options = new HashMap<>();

    /**
     * Mocks the Quarkus-backed scope: {@code get} takes the option suffix, {@code getPropertyNames} returns
     * full {@code kc.spi-events-listener--kafka-events--*} keys (docs/RESEARCH.md §6.1).
     */
    private Config.Scope scope() {
        Config.Scope scope = mock(Config.Scope.class);
        when(scope.get(anyString())).thenAnswer(invocation -> options.get(invocation.<String>getArgument(0)));
        when(scope.getPropertyNames()).thenAnswer(invocation -> options.keySet().stream()
                .map(key -> "kc." + PREFIX + key)
                .collect(Collectors.toSet()));
        return scope;
    }

    private KafkaEventsConfig load() {
        return KafkaEventsConfig.from(scope(), HOST).orElseThrow();
    }

    private KafkaEventsConfigException loadFails() {
        return assertThrows(KafkaEventsConfigException.class, this::load);
    }

    private KafkaEventsConfig loadConfigured() {
        options.putIfAbsent("bootstrap-servers", "kafka:9092");
        return load();
    }

    private KafkaEventsConfigException loadConfiguredFails() {
        options.putIfAbsent("bootstrap-servers", "kafka:9092");
        return loadFails();
    }

    private Path propertiesFile(String content) throws IOException {
        Path file = tempDir.resolve("producer.properties");
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    // --- disabled ----------------------------------------------------------------------------------------

    @Test
    void disabledWhenBootstrapServersMissing() {
        assertThat(KafkaEventsConfig.from(scope(), HOST), is(Optional.empty()));
    }

    @Test
    void disabledWhenBootstrapServersBlank() {
        options.put("bootstrap-servers", "   ");

        assertThat(KafkaEventsConfig.from(scope(), HOST), is(Optional.empty()));
    }

    @Test
    void disabledIgnoresOtherOptionsEvenIfInvalid() {
        // Deploying the JAR unconfigured must be harmless: nothing else is read without bootstrap servers.
        options.put("queue-capacity", "not-a-number");
        options.put("producer-no-such-key", "x");

        assertThat(KafkaEventsConfig.from(scope(), HOST), is(Optional.empty()));
    }

    // --- defaults ----------------------------------------------------------------------------------------

    @Test
    void defaults() {
        KafkaEventsConfig config = loadConfigured();

        assertThat(config.deliveryMode(), is(DeliveryMode.ASYNC));
        assertThat(config.topicEvents(), equalTo("keycloak.events"));
        assertThat(config.topicAdminEvents(), equalTo("keycloak.admin-events"));
        assertThat(config.topicOverrides(), is(anEmptyMap()));
        assertThat(config.includeEventTypes(), is(empty()));
        assertThat(config.excludeEventTypes(), is(empty()));
        assertThat(config.includeAdminResourceTypes(), is(empty()));
        assertThat(config.excludeAdminResourceTypes(), is(empty()));
        assertThat(config.includeIpAddress(), is(true));
        assertThat(config.includeRepresentation(), is(false));
        assertThat(config.allowedDetailKeys(), is(empty()));
        assertThat(config.redactedDetailKeys(), is(empty()));
        assertThat(config.queueCapacity(), is(10_000));
        assertThat(config.closeTimeout(), equalTo(Duration.ofMillis(5_000)));
        assertThat(config.bootstrapServers(), equalTo("kafka:9092"));
        assertThat(config.producerProperties(), equalTo(Map.of(
                "bootstrap.servers", "kafka:9092",
                "acks", "all",
                "enable.idempotence", "true",
                "linger.ms", "20",
                "compression.type", "lz4",
                "max.block.ms", "1000",
                "request.timeout.ms", "10000",
                "delivery.timeout.ms", "30000",
                "client.id", "keycloak-kc-1")));
    }

    @Test
    void defaultProducerPropertiesAreAcceptedByKafka() {
        // Guards against defaults Kafka rejects at construction time (e.g. delivery.timeout.ms <
        // linger.ms + request.timeout.ms). No broker needed: the producer connects lazily.
        Map<String, Object> props = new HashMap<>(loadConfigured().producerProperties());
        props.put("bootstrap.servers", "localhost:1");

        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer())) {
            producer.close(Duration.ZERO);
        }
    }

    @Test
    void valuesAreTrimmed() {
        options.put("bootstrap-servers", "  kafka:9092 ");
        options.put("topic-events", " custom.events ");

        KafkaEventsConfig config = load();

        assertThat(config.bootstrapServers(), equalTo("kafka:9092"));
        assertThat(config.topicEvents(), equalTo("custom.events"));
    }

    // --- client.id ---------------------------------------------------------------------------------------

    @Test
    void clientIdFallsBackWhenHostnameUnavailable() {
        assertThat(ProducerProperties.clientId(() -> null), equalTo("keycloak-kafka-events"));
        assertThat(ProducerProperties.clientId(() -> "  "), equalTo("keycloak-kafka-events"));
        assertThat(ProducerProperties.clientId(() -> {
            throw new IllegalStateException("no network");
        }), equalTo("keycloak-kafka-events"));
    }

    @Test
    void clientIdIsSanitized() {
        assertThat(ProducerProperties.clientId(() -> "kc 1/eu:west"), equalTo("keycloak-kc-1-eu-west"));
    }

    // --- topics ------------------------------------------------------------------------------------------

    @Test
    void topicTemplatesAreKeptRaw() {
        options.put("topic-events", "kc.{realmName}.events");
        options.put("topic-admin-events", "kc.{realmId}.admin");

        KafkaEventsConfig config = loadConfigured();

        assertThat(config.topicEvents(), equalTo("kc.{realmName}.events"));
        assertThat(config.topicAdminEvents(), equalTo("kc.{realmId}.admin"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"topic-events", "topic-admin-events"})
    void blankTopicIsRejected(String option) {
        options.put(option, " ");

        assertThat(loadConfiguredFails().getMessage(), containsString(PREFIX + option));
    }

    @Test
    void topicOverridesAreParsed() {
        options.put("topic-overrides", "LOGIN_ERROR=keycloak.security, register = kc.{realmId}.signups,");

        assertThat(loadConfigured().topicOverrides(), equalTo(Map.of(
                EventType.LOGIN_ERROR, "keycloak.security",
                EventType.REGISTER, "kc.{realmId}.signups")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LOGIN", "LOGIN=", "=topic", "NOT_A_TYPE=topic", "LOGIN=a,LOGIN=b"})
    void malformedTopicOverrideIsRejected(String value) {
        options.put("topic-overrides", value);

        assertThat(loadConfiguredFails().getMessage(), containsString(PREFIX + "topic-overrides"));
    }

    // --- filters -----------------------------------------------------------------------------------------

    @Test
    void eventTypeFiltersAreParsedCaseInsensitively() {
        options.put("include-event-types", "LOGIN, login_error,,");
        options.put("exclude-event-types", "CODE_TO_TOKEN");

        KafkaEventsConfig config = loadConfigured();

        assertThat(config.includeEventTypes(), containsInAnyOrder(EventType.LOGIN, EventType.LOGIN_ERROR));
        assertThat(config.excludeEventTypes(), containsInAnyOrder(EventType.CODE_TO_TOKEN));
    }

    @ParameterizedTest
    @ValueSource(strings = {"include-event-types", "exclude-event-types"})
    void unknownEventTypeIsRejected(String option) {
        options.put(option, "LOGIN,LOGNI");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + option), containsString("LOGNI")));
    }

    @Test
    void adminResourceTypesAreUpperCasedButNotValidated() {
        options.put("include-admin-resource-types", "user, Group, my_custom_resource");
        options.put("exclude-admin-resource-types", "realm_role");

        KafkaEventsConfig config = loadConfigured();

        assertThat(config.includeAdminResourceTypes(), containsInAnyOrder("USER", "GROUP", "MY_CUSTOM_RESOURCE"));
        assertThat(config.excludeAdminResourceTypes(), containsInAnyOrder("REALM_ROLE"));
    }

    @Test
    void detailKeysAreLowerCased() {
        options.put("allowed-detail-keys", "Custom_Required_Action, reason");
        options.put("redacted-detail-keys", "MY_SECRET_HINT");

        KafkaEventsConfig config = loadConfigured();

        assertThat(config.allowedDetailKeys(), containsInAnyOrder("custom_required_action", "reason"));
        assertThat(config.redactedDetailKeys(), containsInAnyOrder("my_secret_hint"));
    }

    // --- booleans ----------------------------------------------------------------------------------------

    @Test
    void booleansCanBeSwitched() {
        options.put("include-ip-address", "false");
        options.put("include-representation", "TRUE");

        KafkaEventsConfig config = loadConfigured();

        assertThat(config.includeIpAddress(), is(false));
        assertThat(config.includeRepresentation(), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"include-ip-address", "include-representation"})
    void invalidBooleanIsRejected(String option) {
        options.put(option, "yes");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + option), containsString("'yes'")));
    }

    // --- delivery mode -----------------------------------------------------------------------------------

    @Test
    void asyncDeliveryModeIsAccepted() {
        options.put("delivery-mode", "ASYNC");

        assertThat(loadConfigured().deliveryMode(), is(DeliveryMode.ASYNC));
    }

    @Test
    void outboxDeliveryModeIsNotSupportedYet() {
        options.put("delivery-mode", "outbox");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "delivery-mode"), containsString("not supported yet")));
    }

    @Test
    void unknownDeliveryModeIsRejected() {
        options.put("delivery-mode", "sync");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "delivery-mode"), containsString("'sync'")));
    }

    // --- numbers -----------------------------------------------------------------------------------------

    @Test
    void numbersAreParsed() {
        options.put("queue-capacity", "1");
        options.put("close-timeout-ms", "0");

        KafkaEventsConfig config = loadConfigured();

        assertThat(config.queueCapacity(), is(1));
        assertThat(config.closeTimeout(), equalTo(Duration.ZERO));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc", "1.5", "99999999999"})
    void invalidQueueCapacityIsRejected(String value) {
        options.put("queue-capacity", value);

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "queue-capacity"), containsString("'" + value + "'")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "abc", "5s"})
    void invalidCloseTimeoutIsRejected(String value) {
        options.put("close-timeout-ms", value);

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "close-timeout-ms"), containsString("'" + value + "'")));
    }

    // --- producer passthrough ----------------------------------------------------------------------------

    @Test
    void passthroughOptionsMapDashesToDots() {
        options.put("producer-linger-ms", "50");
        options.put("producer-security-protocol", "SASL_SSL");
        options.put("producer-sasl-mechanism", "SCRAM-SHA-512");
        options.put("producer-sasl-jaas-config", "org.apache.kafka.common.security.scram.ScramLoginModule required;");

        Map<String, String> props = loadConfigured().producerProperties();

        assertThat(props, hasEntry("linger.ms", "50"));
        assertThat(props, hasEntry("security.protocol", "SASL_SSL"));
        assertThat(props, hasEntry("sasl.mechanism", "SCRAM-SHA-512"));
        assertThat(props, hasEntry("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required;"));
    }

    @Test
    void ownOptionsAreNotPassedThrough() {
        options.put("queue-capacity", "5");
        options.put("topic-events", "t");

        assertThat(loadConfigured().producerProperties(), aMapWithSize(9));
    }

    @Test
    void propertiesFileOptionIsNotTreatedAsPassthrough() throws IOException {
        options.put("producer-properties-file", propertiesFile("linger.ms=7\n").toString());

        Map<String, String> props = loadConfigured().producerProperties();

        assertThat(props, not(hasKey("properties.file")));
        assertThat(props, hasEntry("linger.ms", "7"));
    }

    @Test
    void unknownKafkaKeyIsRejected() {
        options.put("producer-linger-msec", "5");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "producer-linger-msec"), containsString("linger.msec")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"producer-client.id", "producer-Linger-ms", "producer-", "producer-linger--ms"})
    void malformedPassthroughOptionIsRejected(String option) {
        options.put(option, "5");

        assertThat(loadConfiguredFails().getMessage(), containsString(PREFIX + option));
    }

    @Test
    void sslProviderPassthroughPointsToPropertiesFile() {
        options.put("producer-ssl-provider", "SunJSSE");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(
                containsString(PREFIX + "producer-ssl-provider"),
                containsString(PREFIX + "producer-properties-file")));
    }

    @Test
    void sslProviderIsAllowedInPropertiesFile() throws IOException {
        options.put("producer-properties-file", propertiesFile("ssl.provider=SunJSSE\n").toString());

        assertThat(loadConfigured().producerProperties(), hasEntry("ssl.provider", "SunJSSE"));
    }

    @Test
    void invalidPassthroughValueIsRejected() {
        options.put("producer-linger-ms", "soon");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(
                containsString(PREFIX + "producer-linger-ms"),
                containsString("linger.ms"),
                containsString("'soon'")));
    }

    @Test
    void invalidValueOfSensitiveKeyIsMasked() {
        // partitioner.ignore.keys is a BOOLEAN whose name matches the sensitive-key rule ("key").
        options.put("producer-partitioner-ignore-keys", "s3cr3t");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "producer-partitioner-ignore-keys"),
                containsString("****"), not(containsString("s3cr3t"))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"producer-key-serializer", "producer-value-serializer"})
    void serializerPassthroughIsRejected(String option) {
        options.put(option, "org.apache.kafka.common.serialization.StringSerializer");

        assertThat(loadConfiguredFails().getMessage(), containsString(PREFIX + option));
    }

    @ParameterizedTest
    @ValueSource(strings = {"key.serializer", "value.serializer"})
    void serializerInPropertiesFileIsRejected(String key) throws IOException {
        Path file = propertiesFile(key + "=org.apache.kafka.common.serialization.StringSerializer\n");
        options.put("producer-properties-file", file.toString());

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(
                containsString(PREFIX + "producer-properties-file"),
                containsString(file.toString()),
                containsString(key)));
    }

    // --- producer-properties-file ------------------------------------------------------------------------

    @Test
    void missingPropertiesFileIsRejected() {
        Path missing = tempDir.resolve("nope.properties");
        options.put("producer-properties-file", missing.toString());

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "producer-properties-file"),
                containsString(missing.toString())));
    }

    @Test
    void directoryAsPropertiesFileIsRejected() {
        options.put("producer-properties-file", tempDir.toString());

        assertThat(loadConfiguredFails().getMessage(), containsString(PREFIX + "producer-properties-file"));
    }

    @Test
    void unknownKeysInPropertiesFileArePassedOn() throws IOException {
        // Config providers and plugin-specific settings are legitimate non-ProducerConfig keys.
        options.put("producer-properties-file", propertiesFile("""
                config.providers=file
                config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
                """).toString());

        assertThat(loadConfigured().producerProperties(), hasEntry("config.providers.file.class",
                "org.apache.kafka.common.config.provider.FileConfigProvider"));
    }

    @Test
    void invalidValueInPropertiesFileIsRejected() throws IOException {
        Path file = propertiesFile("acks=most\n");
        options.put("producer-properties-file", file.toString());

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString(PREFIX + "producer-properties-file"), containsString("acks")));
    }

    // --- precedence --------------------------------------------------------------------------------------

    @Test
    void precedenceIsDefaultsThenFileThenPassthroughThenBootstrapOption() throws IOException {
        options.put("bootstrap-servers", "option:9092");
        options.put("producer-properties-file", propertiesFile("""
                bootstrap.servers=file:9092
                linger.ms=100
                compression.type=gzip
                security.protocol=SSL
                """).toString());
        options.put("producer-compression-type", "zstd");
        options.put("producer-bootstrap-servers", "passthrough:9092");

        Map<String, String> props = load().producerProperties();

        assertThat(props, hasEntry("acks", "all"));                 // default
        assertThat(props, hasEntry("linger.ms", "100"));            // file over default
        assertThat(props, hasEntry("security.protocol", "SSL"));    // file only
        assertThat(props, hasEntry("compression.type", "zstd"));    // passthrough over file
        assertThat(props, hasEntry("bootstrap.servers", "option:9092")); // option over everything
    }

    @Test
    void userAcksDropsExplicitIdempotenceDefault() {
        // An explicit enable.idempotence=true would make Kafka reject acks=1.
        options.put("producer-acks", "1");

        Map<String, Object> props = new HashMap<>(loadConfigured().producerProperties());

        assertThat(props, not(hasKey("enable.idempotence")));
        props.put("bootstrap.servers", "localhost:1");
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer())) {
            producer.close(Duration.ZERO);
        }
    }

    @Test
    void explicitIdempotenceIsKept() {
        options.put("producer-acks", "all");
        options.put("producer-enable-idempotence", "true");

        assertThat(loadConfigured().producerProperties(), hasEntry("enable.idempotence", "true"));
    }

    @Test
    void inconsistentDeliveryTimeoutIsRejected() {
        options.put("producer-request-timeout-ms", "30000");

        String message = loadConfiguredFails().getMessage();

        assertThat(message, allOf(containsString("delivery.timeout.ms"), containsString("request.timeout.ms")));
    }

    // --- secrets -----------------------------------------------------------------------------------------

    @Test
    void toStringMasksSensitiveProducerValues() throws IOException {
        options.put("producer-properties-file", propertiesFile("""
                ssl.truststore.password=trust-pw
                ssl.keystore.key=-----BEGIN PRIVATE KEY-----
                """).toString());
        options.put("producer-sasl-jaas-config", "LoginModule required password=\"jaas-pw\";");
        options.put("producer-sasl-oauthbearer-token-endpoint-url", "https://idp/token-secret-path");
        options.put("producer-linger-ms", "42");

        String text = loadConfigured().toString();

        assertThat(text, allOf(
                not(containsString("trust-pw")),
                not(containsString("PRIVATE KEY")),
                not(containsString("jaas-pw")),
                not(containsString("token-secret-path")),
                containsString("sasl.jaas.config=****"),
                containsString("linger.ms=42"),
                containsString("bootstrap.servers=kafka:9092")));
    }

    @Test
    void sensitiveKeyDetection() {
        assertThat(ProducerProperties.isSensitive("sasl.jaas.config"), is(true));
        assertThat(ProducerProperties.isSensitive("ssl.key.password"), is(true));
        assertThat(ProducerProperties.isSensitive("sasl.oauthbearer.client.credentials.client.secret"), is(true));
        assertThat(ProducerProperties.isSensitive("SSL.TRUSTSTORE.PASSWORD"), is(true));
        assertThat(ProducerProperties.isSensitive("linger.ms"), is(false));
        assertThat(ProducerProperties.isSensitive("security.protocol"), is(false));
    }

    // --- invariants --------------------------------------------------------------------------------------

    @Test
    void optionNamesAvoidKeycloakBuildTimeSuffixes() {
        // Keycloak treats kc.spi-* keys ending like this as build-time options and refuses
        // `start --optimized` when they are set at runtime (docs/RESEARCH.md §6.1).
        Set<String> offending = KafkaEventsConst.OPTIONS.stream()
                .filter(name -> name.endsWith("-enabled") || name.endsWith("-provider")
                        || name.endsWith("-provider-default"))
                .collect(Collectors.toSet());

        assertThat(offending, is(empty()));
    }

    @Test
    void configIsImmutable() {
        options.put("include-event-types", "LOGIN");
        KafkaEventsConfig config = loadConfigured();

        assertThrows(UnsupportedOperationException.class, () -> config.producerProperties().put("acks", "0"));
        assertThrows(UnsupportedOperationException.class, () -> config.includeEventTypes().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> config.topicOverrides().put(EventType.LOGIN, "x"));
    }
}
