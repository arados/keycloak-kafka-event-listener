package hr.delmisoft.keycloak.kafka.config;

import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_PRODUCER_PREFIX;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_PRODUCER_PROPERTIES_FILE;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.PROPERTY_NAME_PREFIX;
import static hr.delmisoft.keycloak.kafka.config.OptionReader.optionName;

import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.keycloak.Config;

/**
 * Builds the Kafka producer properties from three layers, later ones winning:
 * <ol>
 *   <li>our defaults ({@link #defaults(String)}),</li>
 *   <li>the {@code producer-properties-file},</li>
 *   <li>{@code producer-<kafka-key-with-dashes>} passthrough options,</li>
 * </ol>
 * and finally {@code bootstrap.servers} from the {@code bootstrap-servers} option, which always wins over a
 * {@code bootstrap.servers} in the file or a {@code producer-bootstrap-servers} option.
 *
 * <p>{@code key.serializer} / {@code value.serializer} are rejected from every source: the sender passes
 * serializer instances, which keeps producer creation independent of class loading by name.
 *
 * <p>Values are never logged and never appear in exception messages for {@linkplain #isSensitive sensitive}
 * keys.
 */
final class ProducerProperties {

    static final String MASK = "****";

    static final String FALLBACK_CLIENT_ID = "keycloak-kafka-events";

    private static final String SERIALIZER_REASON =
            "the listener sets its own serializers; remove this setting";

    private static final Pattern SENSITIVE = Pattern.compile("password|secret|jaas|token|key");

    /** Characters of a passthrough suffix after {@code producer-}: Kafka keys are {@code [a-z0-9.]+}. */
    private static final Pattern PASSTHROUGH_SUFFIX_CHARS = Pattern.compile("[a-z0-9-]+");

    /**
     * Keycloak treats SPI keys with these suffixes as build-time options ({@code PropertyMappers
     * .isSpiBuildTimeProperty}): setting one at runtime makes {@code start --optimized} exit.
     */
    private static final List<String> BUILD_TIME_SUFFIXES = List.of("-enabled", "-provider", "-provider-default");

    private static final Map<String, ConfigDef.ConfigKey> KAFKA_KEYS = ProducerConfig.configDef().configKeys();

    /**
     * Settings Kafka silently reconciles with its default idempotence (by turning it off) but rejects when
     * {@code enable.idempotence=true} is set explicitly (e.g. {@code acks=1}, {@code retries=0}).
     */
    private static final List<String> IDEMPOTENCE_CONFLICTS = List.of(
            ProducerConfig.ACKS_CONFIG,
            ProducerConfig.RETRIES_CONFIG);

    private ProducerProperties() {
    }

    /** True for keys whose values must never be shown (passwords, JAAS config, tokens, key material). */
    static boolean isSensitive(String kafkaKey) {
        return SENSITIVE.matcher(kafkaKey.toLowerCase(Locale.ROOT)).find();
    }

    /** The value as it may be displayed: masked for {@linkplain #isSensitive sensitive} keys. */
    static String display(String kafkaKey, String value) {
        return isSensitive(kafkaKey) ? MASK : value;
    }

    /** Our producer defaults; any of them can be overridden by the file or a passthrough option. */
    static Map<String, String> defaults(String clientId) {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put(ProducerConfig.ACKS_CONFIG, "all");
        defaults.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        defaults.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        // Only lz4 is bundled (PLAN D4); zstd/snappy need their JARs added to providers/.
        defaults.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        // send() runs on our sender thread, never on a request thread; still keep metadata waits short.
        defaults.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        // Kafka requires delivery.timeout.ms >= linger.ms + request.timeout.ms when delivery.timeout.ms is
        // set explicitly; its request.timeout.ms default (30000) would make our delivery.timeout.ms invalid.
        defaults.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        defaults.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        defaults.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return defaults;
    }

    static Map<String, String> build(Config.Scope scope, String bootstrapServers, Supplier<String> hostname) {
        Map<String, String> user = new LinkedHashMap<>();
        user.putAll(readFile(scope));
        user.putAll(readPassthrough(scope));

        Map<String, String> merged = defaults(clientId(hostname));
        if (!user.containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
                && IDEMPOTENCE_CONFLICTS.stream().anyMatch(user::containsKey)) {
            // An explicit enable.idempotence=true makes Kafka reject e.g. acks=1. Without it, Kafka's own
            // default (idempotence on) applies and is silently turned off when the user's settings conflict.
            merged.remove(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        }
        merged.putAll(user);
        merged.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        checkDeliveryTimeout(merged);
        return merged;
    }

    // --- producer-properties-file ------------------------------------------------------------------------

    private static Map<String, String> readFile(Config.Scope scope) {
        Map<String, String> result = new LinkedHashMap<>();
        String location = scope.get(CFG_PRODUCER_PROPERTIES_FILE);
        if (location == null || location.isBlank()) {
            return result;
        }
        location = location.trim();
        String option = optionName(CFG_PRODUCER_PROPERTIES_FILE);
        Path path = Path.of(location);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new KafkaEventsConfigException(
                    "Cannot read " + option + " '" + location + "': not a readable regular file");
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException | IllegalArgumentException e) {
            // IOException messages carry the path or charset problem, never file contents.
            throw new KafkaEventsConfigException(
                    "Cannot read " + option + " '" + location + "': " + e.getClass().getSimpleName());
        }
        String source = option + " '" + location + "'";
        for (String key : new TreeSet<>(properties.stringPropertyNames())) {
            String kafkaKey = key.trim();
            String value = properties.getProperty(key);
            rejectSerializer(kafkaKey, source);
            // Unknown keys are allowed here: config providers, interceptor and callback-handler settings
            // are legitimate custom keys. Kafka itself warns about keys nothing uses.
            validateValue(kafkaKey, value, source + " (key " + kafkaKey + ")");
            result.put(kafkaKey, value);
        }
        return result;
    }

    // --- producer-* passthrough --------------------------------------------------------------------------

    private static Map<String, String> readPassthrough(Config.Scope scope) {
        Map<String, String> result = new LinkedHashMap<>();
        // Sorted for deterministic error reporting.
        for (String propertyName : new TreeSet<>(scope.getPropertyNames())) {
            if (!propertyName.startsWith(PROPERTY_NAME_PREFIX)) {
                continue;
            }
            String key = propertyName.substring(PROPERTY_NAME_PREFIX.length());
            if (!key.startsWith(CFG_PRODUCER_PREFIX) || key.equals(CFG_PRODUCER_PROPERTIES_FILE)) {
                continue;
            }
            String option = optionName(key);
            String suffix = key.substring(CFG_PRODUCER_PREFIX.length());
            if (!isPassthroughSuffix(suffix)) {
                throw new KafkaEventsConfigException("Invalid option " + option
                        + ": producer passthrough options must be the Kafka key in lower case with dashes"
                        + " instead of dots, e.g. " + optionName(CFG_PRODUCER_PREFIX + "linger-ms"));
            }
            if (BUILD_TIME_SUFFIXES.stream().anyMatch(key::endsWith)) {
                throw new KafkaEventsConfigException("Option " + option + " cannot be used: Keycloak treats"
                        + " SPI options ending in -enabled, -provider or -provider-default as build-time"
                        + " options. Set it in the file given by " + optionName(CFG_PRODUCER_PROPERTIES_FILE)
                        + " instead");
            }
            String kafkaKey = suffix.replace('-', '.');
            rejectSerializer(kafkaKey, option);
            if (!KAFKA_KEYS.containsKey(kafkaKey)) {
                throw new KafkaEventsConfigException("Unknown option " + option + ": '" + kafkaKey
                        + "' is not a Kafka producer setting");
            }
            String value = scope.get(key);
            if (value == null) {
                // Listed but empty: SmallRye reports empty values as missing. Nothing to pass on.
                continue;
            }
            validateValue(kafkaKey, value, option);
            result.put(kafkaKey, value);
        }
        return result;
    }

    /** Lower-case dash-separated words, i.e. a Kafka key with dots replaced by dashes. */
    private static boolean isPassthroughSuffix(String suffix) {
        return PASSTHROUGH_SUFFIX_CHARS.matcher(suffix).matches()
                && !suffix.startsWith("-") && !suffix.endsWith("-") && !suffix.contains("--");
    }

    // --- validation --------------------------------------------------------------------------------------

    private static void rejectSerializer(String kafkaKey, String source) {
        if (kafkaKey.equals(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                || kafkaKey.equals(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            throw new KafkaEventsConfigException(
                    "Invalid setting " + kafkaKey + " in " + source + ": " + SERIALIZER_REASON);
        }
    }

    /**
     * Type-checks a value for a known Kafka key so typos fail at startup, not when the sender starts.
     * Class names are not loaded here (wrong classloader context, and they may live in other providers).
     */
    private static void validateValue(String kafkaKey, String value, String source) {
        ConfigDef.ConfigKey definition = KAFKA_KEYS.get(kafkaKey);
        if (definition == null || definition.type == ConfigDef.Type.CLASS) {
            return;
        }
        try {
            Object parsed = ConfigDef.parseType(kafkaKey, value, definition.type);
            if (definition.validator != null) {
                definition.validator.ensureValid(kafkaKey, parsed);
            }
        } catch (ConfigException e) {
            // Kafka's message echoes the value, so only include it for non-sensitive keys.
            String detail = isSensitive(kafkaKey) ? "" : ": " + e.getMessage();
            throw new KafkaEventsConfigException("Invalid value for " + source + " (Kafka " + kafkaKey + "): '"
                    + display(kafkaKey, value) + "'" + detail);
        }
    }

    private static void checkDeliveryTimeout(Map<String, String> merged) {
        long delivery = Long.parseLong(merged.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG).trim());
        long linger = Long.parseLong(merged.get(ProducerConfig.LINGER_MS_CONFIG).trim());
        long request = Long.parseLong(merged.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG).trim());
        if (delivery < linger + request) {
            throw new KafkaEventsConfigException("Invalid Kafka producer settings: delivery.timeout.ms ("
                    + delivery + ") must be >= linger.ms (" + linger + ") + request.timeout.ms (" + request
                    + "); adjust them via " + optionName(CFG_PRODUCER_PREFIX + "...") + " or "
                    + optionName(CFG_PRODUCER_PROPERTIES_FILE));
        }
    }

    // --- client.id ---------------------------------------------------------------------------------------

    static String clientId(Supplier<String> hostname) {
        String host;
        try {
            host = hostname.get();
        } catch (RuntimeException e) {
            host = null;
        }
        if (host == null) {
            return FALLBACK_CLIENT_ID;
        }
        // client.id ends up in Kafka metric and JMX names; keep it to a conservative character set.
        String sanitized = host.trim().replaceAll("[^A-Za-z0-9._-]", "-");
        return sanitized.isEmpty() ? FALLBACK_CLIENT_ID : "keycloak-" + sanitized;
    }

    /** {@code HOSTNAME} (set in containers) or the local host name; {@code null} if neither is available. */
    static String localHostname() {
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (IOException | SecurityException e) {
            return null;
        }
    }
}
