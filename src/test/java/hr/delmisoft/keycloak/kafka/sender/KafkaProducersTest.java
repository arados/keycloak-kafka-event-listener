package hr.delmisoft.keycloak.kafka.sender;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class KafkaProducersTest {

    /** Nothing listens on port 1; constructing a producer must not need a broker. */
    private static final String UNREACHABLE_BOOTSTRAP = "127.0.0.1:1";

    private static final ClassLoader PLUGIN_LOADER = KafkaProducers.class.getClassLoader();

    /** Stands in for the Keycloak thread's own TCCL, which differs from the plugin's classloader. */
    private final URLClassLoader callerLoader = new URLClassLoader(new URL[0], null);

    private ClassLoader original;

    @BeforeEach
    void setCallerContextClassLoader() {
        original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(callerLoader);
    }

    @AfterEach
    void restoreContextClassLoader() throws IOException {
        Thread.currentThread().setContextClassLoader(original);
        callerLoader.close();
    }

    @Test
    void constructsWithThePluginClassLoaderAsContextAndRestoresIt() {
        AtomicReference<ClassLoader> during = new AtomicReference<>();
        AtomicReference<Map<String, Object>> config = new AtomicReference<>();
        MockProducer<String, byte[]> mock = new MockProducer<>();

        Producer<String, byte[]> producer = KafkaProducers.create(Map.of("linger.ms", "20"), cfg -> {
            during.set(Thread.currentThread().getContextClassLoader());
            config.set(cfg);
            return mock;
        });

        assertThat(producer, sameInstance(mock));
        assertThat(during.get(), sameInstance(PLUGIN_LOADER));
        assertThat(config.get(), equalTo(Map.<String, Object>of("linger.ms", "20")));
        assertThat(Thread.currentThread().getContextClassLoader(), sameInstance(callerLoader));
    }

    @Test
    void restoresTheContextClassLoaderWhenConstructionFails() {
        assertThrows(ProducerCreationException.class, () -> KafkaProducers.create(Map.of(), cfg -> {
            throw new KafkaException("Failed to construct kafka producer");
        }));

        assertThat(Thread.currentThread().getContextClassLoader(), sameInstance(callerLoader));
    }

    @Test
    void createsARealKafkaProducerWithoutABroker() {
        Producer<String, byte[]> producer = KafkaProducers.create(properties("lz4"));
        try {
            assertThat(producer, instanceOf(KafkaProducer.class));
            assertThat(Thread.currentThread().getContextClassLoader(), sameInstance(callerLoader));
        } finally {
            producer.close(Duration.ofMillis(100));
        }
    }

    @ParameterizedTest
    @CsvSource({"zstd, com.github.luben:zstd-jni:1.5.7-17", "snappy, org.xerial.snappy:snappy-java:1.1.10.7"})
    void missingCodecJarIsNamedInTheError(String compressionType, String jar) {
        var thrown = assertThrows(ProducerCreationException.class,
                () -> KafkaProducers.create(properties(compressionType)));

        assertThat(thrown.getMessage(), allOf(
                containsString("compression.type=" + compressionType),
                containsString(jar),
                containsString("providers/"),
                containsString("kc.sh build")));
        assertThat(thrown.getCause(), instanceOf(KafkaException.class));
        assertThat(Thread.currentThread().getContextClassLoader(), sameInstance(callerLoader));
    }

    @Test
    void otherFailuresKeepTheCauseButNoSettingValuesInTheMessage() {
        var cause = new KafkaException("Failed to construct kafka producer",
                new ConfigException("Invalid value s3cr3t for configuration sasl.jaas.config"));

        var thrown = assertThrows(ProducerCreationException.class,
                () -> KafkaProducers.create(Map.of("sasl.jaas.config", "s3cr3t"), cfg -> {
                    throw cause;
                }));

        assertThat(thrown.getMessage(), allOf(
                containsString("Cannot create the Kafka producer"),
                containsString(ConfigException.class.getName()),
                not(containsString("s3cr3t"))));
        assertThat(thrown.getCause(), sameInstance(cause));
    }

    @Test
    void codecCoordinatesMatchTheReadme() throws IOException {
        // The error message tells operators which JAR to add; it must agree with the documented recipe.
        String readme = Files.readString(Path.of("README.md"), StandardCharsets.UTF_8);

        assertThat(readme, allOf(containsString(KafkaProducers.ZSTD_JAR), containsString(KafkaProducers.SNAPPY_JAR)));
    }

    private static Map<String, String> properties(String compressionType) {
        Map<String, String> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, UNREACHABLE_BOOTSTRAP);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        return properties;
    }
}
