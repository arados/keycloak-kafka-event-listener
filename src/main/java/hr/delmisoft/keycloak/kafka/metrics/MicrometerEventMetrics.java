package hr.delmisoft.keycloak.kafka.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * {@link EventMetrics} on a Micrometer registry, by default Keycloak's {@link Metrics#globalRegistry}, which
 * Keycloak exposes on its management interface ({@code /metrics}) when started with {@code metrics-enabled=true}.
 * Without that, increments land in an empty composite registry and cost next to nothing.
 *
 * <p>Meters (Prometheus names): {@code keycloak_kafka_events_enqueued_total{kind}},
 * {@code _sent_total{kind,topic}}, {@code _failed_total{kind,reason}}, {@code _dropped_total{kind,reason}},
 * {@code _dead_lettered_total{kind}}, gauges {@code _queue_size} and {@code _queue_capacity}; outbox gauges are
 * registered with {@link #gauge}. Counters are cached per tag combination, so the hot path is one map lookup and
 * an add. {@link #close()} removes every meter this instance registered (the factory is closed on shutdown and
 * when Keycloak reloads providers). Thread-safe.
 */
public final class MicrometerEventMetrics implements EventMetrics, AutoCloseable {

    public static final String PREFIX = "keycloak.kafka.events.";

    static final String ENQUEUED = PREFIX + "enqueued";
    static final String SENT = PREFIX + "sent";
    static final String FAILED = PREFIX + "failed";
    static final String DROPPED = PREFIX + "dropped";
    static final String DEAD_LETTERED = PREFIX + "dead.lettered";
    static final String QUEUE_SIZE = PREFIX + "queue.size";
    static final String QUEUE_CAPACITY = PREFIX + "queue.capacity";

    private static final String TAG_KIND = "kind";
    private static final String TAG_TOPIC = "topic";
    private static final String TAG_REASON = "reason";

    private record CounterKey(String name, String kind, String tagName, String tagValue) {
    }

    private final MeterRegistry registry;
    private final Map<CounterKey, Counter> counters = new ConcurrentHashMap<>();
    private final List<Meter> registered = new CopyOnWriteArrayList<>();

    private volatile boolean closed;

    /** On Keycloak's global registry. */
    public MicrometerEventMetrics() {
        this(Metrics.globalRegistry);
    }

    public MicrometerEventMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public void enqueued(String kind) {
        increment(ENQUEUED, kind, null, null, "Events accepted for sending to Kafka");
    }

    @Override
    public void sent(String kind, String topic) {
        increment(SENT, kind, TAG_TOPIC, topic, "Events Kafka acknowledged");
    }

    @Override
    public void failed(String kind, String reason) {
        increment(FAILED, kind, TAG_REASON, reason, "Failed attempts to publish an event to Kafka");
    }

    @Override
    public void dropped(String kind, String reason) {
        increment(DROPPED, kind, TAG_REASON, reason, "Events that were not published");
    }

    @Override
    public void deadLettered(String kind) {
        increment(DEAD_LETTERED, kind, null, null, "Outbox events given up on");
    }

    @Override
    public void queue(IntSupplier size, int capacity) {
        gauge(QUEUE_SIZE, "Events waiting in the in-memory queue", () -> size.getAsInt());
        gauge(QUEUE_CAPACITY, "Bound of the in-memory queue (queue-capacity)", () -> capacity);
    }

    /**
     * Registers a gauge that reads {@code value} on every scrape; removed again by {@link #close()}.
     *
     * @param name full meter name, starting with {@link #PREFIX}
     */
    public void gauge(String name, String description, Supplier<Number> value) {
        if (closed) {
            return;
        }
        registered.add(Gauge.builder(name, value).description(description).strongReference(true)
                .register(registry));
    }

    @Override
    public void close() {
        closed = true;
        for (Meter meter : registered) {
            registry.remove(meter);
        }
        registered.clear();
        counters.clear();
    }

    private void increment(String name, String kind, String tagName, String tagValue, String description) {
        if (closed) {
            // Late callbacks (e.g. Kafka's I/O thread) must not re-register meters after close().
            return;
        }
        String kindTag = kind == null ? KIND_UNKNOWN : kind;
        String value = tagValue == null ? "" : tagValue;
        counters.computeIfAbsent(new CounterKey(name, kindTag, tagName, value), key -> {
            Counter.Builder builder = Counter.builder(name).description(description).tag(TAG_KIND, kindTag);
            if (tagName != null) {
                builder.tag(tagName, value);
            }
            Counter counter = builder.register(registry);
            registered.add(counter);
            return counter;
        }).increment();
    }
}
