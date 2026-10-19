package hr.delmisoft.keycloak.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Decision D4: the shaded JAR bundles only the lz4 codec. zstd-jni and snappy-java are excluded from the
 * kafka-clients dependency, so this test classpath matches what Keycloak sees with our JAR alone.
 */
class CompressionCodecsTest {

    /** Nothing listens on port 1; constructing a producer must not need a broker. */
    private static final String UNREACHABLE_BOOTSTRAP = "127.0.0.1:1";

    @ParameterizedTest
    @CsvSource({"com.github.luben.zstd.Zstd", "org.xerial.snappy.Snappy"})
    void zstdAndSnappyAreNotOnTheClasspath(String codecClass) {
        // Guards the pom exclusions; every other test here is only meaningful while this holds.
        assertThrows(ClassNotFoundException.class, () -> Class.forName(codecClass));
    }

    @Test
    void producerWithLz4ConstructsWithoutZstdOrSnappy() {
        var producer = newProducer("lz4");
        producer.close(Duration.ofMillis(100));
    }

    @Test
    void lz4RecordBatchRoundTrips() {
        // Highly compressible values, so the batch is visibly smaller than its payload.
        byte[] key = "realm-1:user-1".getBytes(StandardCharsets.UTF_8);
        byte[] value = "{\"type\":\"LOGIN\"}".repeat(200).getBytes(StandardCharsets.UTF_8);
        var written = MemoryRecords.withRecords(Compression.lz4().build(),
                new SimpleRecord(1_000L, key, value), new SimpleRecord(2_000L, key, value));

        // Re-read from raw bytes, as a consumer would after the wire.
        var read = MemoryRecords.readableRecords(written.buffer().duplicate());
        List<Record> records = new ArrayList<>();
        for (MutableRecordBatch batch : read.batches()) {
            assertThat(batch.compressionType(), equalTo(CompressionType.LZ4));
            batch.forEach(records::add);
        }

        assertThat(read.sizeInBytes(), lessThan(value.length));
        assertThat(records, hasSize(2));
        for (Record record : records) {
            assertThat(bytes(record.key()), equalTo(key));
            assertThat(bytes(record.value()), equalTo(value));
        }
        assertThat(records.get(1).timestamp(), equalTo(2_000L));
    }

    /**
     * Pins down how a missing codec JAR fails, which the config layer relies on: at producer construction
     * (Kafka's compression builder links {@code ZstdCompression}/{@code SnappyCompression}, and bytecode
     * verification loads the codec's stream class), not later on the sender thread at first send.
     */
    @ParameterizedTest
    @CsvSource({"zstd, com/github/luben/zstd/", "snappy, org/xerial/snappy/"})
    void missingCodecJarFailsAtProducerConstruction(String compressionType, String missingPackage) {
        var thrown = assertThrows(KafkaException.class, () -> newProducer(compressionType));

        assertThat(thrown.getMessage(), equalTo("Failed to construct kafka producer"));
        Throwable cause = thrown.getCause();
        assertThat(cause, notNullValue());
        assertThat(cause, instanceOf(NoClassDefFoundError.class));
        assertThat(cause.getMessage(), containsString(missingPackage));
    }

    private static KafkaProducer<String, byte[]> newProducer(String compressionType) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, UNREACHABLE_BOOTSTRAP,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 100);
        return new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer());
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }
}
