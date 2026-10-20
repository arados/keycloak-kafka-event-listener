package hr.delmisoft.keycloak.kafka.outbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import hr.delmisoft.keycloak.kafka.config.OutboxOptions;
import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.metrics.MicrometerEventMetrics;
import hr.delmisoft.keycloak.kafka.outbox.InMemoryOutboxRepository.Status;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import hr.delmisoft.keycloak.kafka.routing.KeyStrategy;
import hr.delmisoft.keycloak.kafka.routing.TopicRouter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakSession;
import org.mockito.Mockito;

class KafkaOutboxDrainerTest {

    private static final Instant NOW = Instant.parse("2026-10-20T12:00:00Z");

    private final InMemoryOutboxRepository table = new InMemoryOutboxRepository();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerEventMetrics metrics = new MicrometerEventMetrics(registry);
    private final EventRecordFactory records =
            new EventRecordFactory(new TopicRouter("keycloak.events", "keycloak.admin-events", Map.of()),
                    new KeyStrategy());
    private OutboxOptions options = OutboxOptions.DEFAULTS;

    @Test
    void deliversCommittedRowsAndDeletesThem() {
        enqueue("event-1");
        enqueue("event-2");
        var producer = autoCompleting();

        run(producer);

        assertThat(producer.history(), hasSize(2));
        assertThat(producer.history().get(0).value(),
                equalTo(table.entries().get(0).payload().getBytes(StandardCharsets.UTF_8)));
        assertThat(producer.history().get(1).headers().lastHeader("kc-kind"), is(not(org.hamcrest.Matchers.nullValue())));
        assertThat(table.rows(), anEmptyMap());
        assertThat(count("keycloak.kafka.events.sent", "kind", "user", "topic", "keycloak.events"), equalTo(2.0));
    }

    @Test
    void withDeliveredRetentionRowsAreKeptAndPurgedLater() {
        options = withDeliveredRetention(Duration.ofMinutes(10));
        String id = enqueue("event-1");

        run(autoCompleting());
        assertThat(table.row(id).status, is(Status.DELIVERED));

        table.now(NOW.plus(Duration.ofMinutes(11)));
        drainer(autoCompleting(), Clock.fixed(NOW.plus(Duration.ofMinutes(11)), ZoneOffset.UTC)).run(session());
        assertThat(table.rows(), anEmptyMap());
    }

    @Test
    void kafkaUnreachableLeavesTheBatchDueWithoutCountingAttempts() {
        String first = enqueue("event-1");
        String second = enqueue("event-2");
        AtomicInteger sends = new AtomicInteger();
        var producer = new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback cb) {
                sends.incrementAndGet();
                throw new TimeoutException("Topic keycloak.events not present in metadata after 1000 ms.");
            }
        };

        run(producer);

        assertThat("stops after the first unreachable send", sends.get(), equalTo(1));
        assertThat(table.row(first).attempts, equalTo(0));
        assertThat(table.row(first).nextAttemptAt, equalTo(NOW.plus(options.drainInterval())));
        assertThat(table.row(first).lastError, containsString("TimeoutException"));
        assertThat(table.row(second).nextAttemptAt, equalTo(NOW));
        assertThat(table.row(second).status, is(Status.PENDING));
        assertThat(count("keycloak.kafka.events.failed", "kind", "user", "reason", "TimeoutException"), equalTo(1.0));
    }

    @Test
    void retriableAcknowledgementErrorsAreRetriedWithoutCountingAttempts() {
        String id = enqueue("event-1");
        var producer = manual();

        drainAsync(producer, () -> producer.errorNext(new NotEnoughReplicasException("not enough")));

        assertThat(table.row(id).status, is(Status.PENDING));
        assertThat(table.row(id).attempts, equalTo(0));
        assertThat(table.row(id).nextAttemptAt, equalTo(NOW.plus(options.drainInterval())));
    }

    @Test
    void recordsKafkaCanNeverAcceptAreDeadLetteredAtOnce() {
        String id = enqueue("event-1");
        var producer = manual();

        drainAsync(producer, () -> producer.errorNext(new RecordTooLargeException("too large")));

        assertThat(table.row(id).status, is(Status.DEAD_LETTER));
        assertThat(table.row(id).lastError, containsString("RecordTooLargeException"));
        assertThat(count("keycloak.kafka.events.dead.lettered", "kind", "user"), equalTo(1.0));
    }

    @Test
    void otherErrorsBackOffAndAreDeadLetteredAfterMaxAttempts() {
        options = new OutboxOptions(Duration.ofSeconds(1), 500, 3, Duration.ZERO, Duration.ofDays(7),
                Duration.ofDays(7));
        String id = enqueue("event-1");

        for (int attempt = 1; attempt <= 3; attempt++) {
            var producer = manual();
            drainAsync(producer, () -> producer.errorNext(new TopicAuthorizationException("denied")));
            if (attempt < 3) {
                assertThat(table.row(id).attempts, equalTo(attempt));
                assertThat(table.row(id).nextAttemptAt, equalTo(NOW.plus(KafkaOutboxDrainer.backoff(attempt))));
                table.row(id).nextAttemptAt = NOW; // due again
            }
        }

        assertThat(table.row(id).status, is(Status.DEAD_LETTER));
        assertThat(table.row(id).lastError, containsString("after 3 attempts"));
    }

    @Test
    void unreadableRowsAreDeadLettered() {
        table.enqueue(new OutboxEntry("realm-1", "event-1", "org.keycloak.event.LOGIN", "user", "{}", "garbage"));

        run(autoCompleting());

        assertThat(table.rows().values().iterator().next().status, is(Status.DEAD_LETTER));
    }

    @Test
    void rowsPendingLongerThanTheMaxAgeAreDeadLettered() {
        String old = enqueue("event-1");
        table.row(old).createdAt = NOW.minus(Duration.ofDays(8));
        table.row(old).nextAttemptAt = NOW.plus(Duration.ofHours(1)); // not due, so not sent

        run(autoCompleting());

        assertThat(table.row(old).status, is(Status.DEAD_LETTER));
        assertThat(table.row(old).lastError, containsString("outbox-pending-max-age-ms"));
    }

    @Test
    void statsAreRefreshedAfterEachRun() {
        enqueue("event-1");
        var drainer = drainer(new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback cb) {
                throw new TimeoutException("down");
            }
        }, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThat(drainer.stats(), equalTo(OutboxStats.EMPTY));
        drainer.run(session());

        assertThat(drainer.stats().pending(), equalTo(1L));
        assertThat(drainer.stats().oldestPendingCreatedAt(), equalTo(NOW));
        assertThat(drainer.getTaskName(), equalTo(KafkaOutboxDrainer.TASK_NAME));
    }

    @Test
    void backoffGrowsAndCapsAtOneHour() {
        assertThat(KafkaOutboxDrainer.backoff(1), equalTo(Duration.ofSeconds(1)));
        assertThat(KafkaOutboxDrainer.backoff(3), equalTo(Duration.ofSeconds(30)));
        assertThat(KafkaOutboxDrainer.backoff(99), equalTo(Duration.ofHours(1)));
    }

    // --- helpers -----------------------------------------------------------------------------------------

    private String enqueue(String eventId) {
        Envelope envelope = OutboxCodecTest.envelope(eventId, "realm-1", "user");
        ProducerRecord<String, byte[]> record = records.create(envelope, EnvelopeMapper.toJsonBytes(envelope));
        table.enqueue(OutboxCodec.encode(envelope, record));
        return table.rows().keySet().stream().reduce((a, b) -> b).orElseThrow();
    }

    private void run(MockProducer<String, byte[]> producer) {
        drainer(producer, Clock.fixed(NOW, ZoneOffset.UTC)).run(session());
    }

    /** Runs the drainer on another thread and completes the in-flight sends with {@code complete}. */
    private void drainAsync(MockProducer<String, byte[]> producer, Runnable complete) {
        Thread thread = new Thread(() -> run(producer));
        thread.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (producer.history().isEmpty() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        complete.run();
        try {
            thread.join(Duration.ofSeconds(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat("drainer finished", thread.isAlive(), is(false));
    }

    private KafkaOutboxDrainer drainer(MockProducer<String, byte[]> producer, Clock clock) {
        return new KafkaOutboxDrainer(s -> table, producer, options, Duration.ofSeconds(30), metrics, clock);
    }

    private static MockProducer<String, byte[]> autoCompleting() {
        return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    }

    private static MockProducer<String, byte[]> manual() {
        return new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
    }

    private static OutboxOptions withDeliveredRetention(Duration retention) {
        OutboxOptions d = OutboxOptions.DEFAULTS;
        return new OutboxOptions(d.drainInterval(), d.batchSize(), d.maxAttempts(), retention,
                d.deadLetterRetention(), d.pendingMaxAge());
    }

    private static KeycloakSession session() {
        return Mockito.mock(KeycloakSession.class);
    }

    private double count(String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }
}
