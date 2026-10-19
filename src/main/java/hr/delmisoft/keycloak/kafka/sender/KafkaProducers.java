package hr.delmisoft.keycloak.kafka.sender;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Creates the real {@link KafkaProducer} in a way that works under Keycloak's provider classloading:
 * <ul>
 *   <li>serializers are passed as instances, so Kafka never loads them by class name;</li>
 *   <li>the thread context classloader is this plugin's classloader while the producer is constructed
 *       (Kafka resolves configured classes such as SASL login/callback handlers, partitioners and
 *       interceptors through the TCCL), and is restored afterwards.</li>
 * </ul>
 * A missing zstd/snappy codec JAR (decision D4, docs/RESEARCH.md §5.1) is reported with the JAR to add.
 */
public final class KafkaProducers {

    /** Codec JARs matching the bundled kafka-clients; keep in sync with README "Compression". */
    static final String ZSTD_JAR = "com.github.luben:zstd-jni:1.5.7-17";

    static final String SNAPPY_JAR = "org.xerial.snappy:snappy-java:1.1.10.7";

    /** Codecs this JAR does not bundle, recognised by the package of the class Kafka failed to load. */
    private enum MissingCodec {
        ZSTD("zstd", "com/github/luben/zstd/", ZSTD_JAR),
        SNAPPY("snappy", "org/xerial/snappy/", SNAPPY_JAR);

        private final String compressionType;
        private final String classPrefix;
        private final String jar;

        MissingCodec(String compressionType, String classPrefix, String jar) {
            this.compressionType = compressionType;
            this.classPrefix = classPrefix;
            this.jar = jar;
        }
    }

    private KafkaProducers() {
    }

    /**
     * Creates a producer from complete producer properties (e.g. {@code KafkaEventsConfig.producerProperties()}).
     *
     * @throws ProducerCreationException if Kafka rejects the configuration or a required class is missing;
     *         the message never contains property values
     */
    public static Producer<String, byte[]> create(Map<String, String> properties) {
        return create(properties, config -> new KafkaProducer<>(config, new StringSerializer(),
                new ByteArraySerializer()));
    }

    static Producer<String, byte[]> create(Map<String, String> properties,
            Function<Map<String, Object>, Producer<String, byte[]>> constructor) {
        Map<String, Object> config = new HashMap<>(properties);
        Thread current = Thread.currentThread();
        ClassLoader previous = current.getContextClassLoader();
        current.setContextClassLoader(KafkaProducers.class.getClassLoader());
        try {
            return constructor.apply(config);
        } catch (RuntimeException | LinkageError e) {
            throw creationFailure(e);
        } finally {
            current.setContextClassLoader(previous);
        }
    }

    static ProducerCreationException creationFailure(Throwable error) {
        Throwable root = error;
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = error; t != null && seen.add(t); t = t.getCause()) {
            root = t;
            MissingCodec codec = missingCodec(t);
            if (codec != null) {
                return new ProducerCreationException("Cannot create the Kafka producer: compression.type="
                        + codec.compressionType + " needs " + codec.jar + ", which this provider JAR does not"
                        + " bundle. Add that JAR to Keycloak's providers/ directory and re-run 'kc.sh build'"
                        + " (see the README section \"Compression\"), or use compression.type lz4, gzip or none",
                        error);
            }
        }
        // Kafka's own messages are in the cause; ours names no setting values, which may be secrets.
        return new ProducerCreationException("Cannot create the Kafka producer (" + root.getClass().getName()
                + "); check the producer settings, see the cause for details", error);
    }

    private static MissingCodec missingCodec(Throwable t) {
        if (!(t instanceof NoClassDefFoundError) && !(t instanceof ClassNotFoundException)) {
            return null;
        }
        String message = t.getMessage();
        if (message == null) {
            return null;
        }
        String className = message.replace('.', '/');
        for (MissingCodec codec : MissingCodec.values()) {
            if (className.contains(codec.classPrefix)) {
                return codec;
            }
        }
        return null;
    }
}
