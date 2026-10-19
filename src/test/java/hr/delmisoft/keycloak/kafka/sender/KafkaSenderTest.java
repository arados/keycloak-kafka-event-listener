package hr.delmisoft.keycloak.kafka.sender;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaSenderTest {

    /** Upper bound for anything that should happen "soon"; only reached when a test fails. */
    private static final Duration WAIT = Duration.ofSeconds(10);

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final List<KafkaSender> senders = new ArrayList<>();
    private final List<GatedProducer> gates = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        gates.forEach(GatedProducer::release);
        senders.forEach(KafkaSender::close);
    }

    @Test
    void publishesOfferedRecordsAndCountsCallbackOutcomes() {
        var producer = new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
        var sender = start(producer, 10, CLOSE_TIMEOUT);

        assertThat(sender.offer(record("t1", "a")), is(true));
        assertThat(sender.offer(record("t2", "b")), is(true));
        assertThat(sender.offer(record("t1", "c")), is(true));
        awaitUntil(() -> producer.history().size() == 3);

        producer.completeNext();
        producer.errorNext(new TimeoutException("Expiring 1 record(s)"));
        producer.errorNext(new RecordTooLargeException("too large"));

        assertThat(producer.history().stream().map(ProducerRecord::topic).toList(), contains("t1", "t2", "t1"));
        SenderStats stats = sender.stats();
        assertThat(stats.enqueued(), equalTo(3L));
        assertThat(stats.sent(), equalTo(1L));
        assertThat(stats.failed("TimeoutException"), equalTo(1L));
        assertThat(stats.failed("RecordTooLargeException"), equalTo(1L));
        assertThat(stats.failedTotal(), equalTo(2L));
        assertThat(stats.droppedTotal(), equalTo(0L));
        assertThat(stats.queueSize(), equalTo(0));
        assertThat(stats.queueCapacity(), equalTo(10));
    }

    @Test
    void offerNeverBlocksWhileTheSenderThreadIsStuckInSend() throws InterruptedException {
        var producer = gated(true);
        var sender = start(producer, 100, CLOSE_TIMEOUT);
        sender.offer(record("t", "first"));
        assertThat(producer.entered.await(WAIT.toSeconds(), TimeUnit.SECONDS), is(true));

        int attempts = 100_000;
        long[] maxOfferNanos = {0};
        int accepted = assertTimeoutPreemptively(WAIT, () -> {
            int count = 0;
            for (int i = 0; i < attempts; i++) {
                long start = System.nanoTime();
                if (sender.offer(record("t", "v" + i))) {
                    count++;
                }
                maxOfferNanos[0] = Math.max(maxOfferNanos[0], System.nanoTime() - start);
            }
            return count;
        });

        // The sender thread holds the first record inside send(); the queue fills, then every offer drops.
        assertThat(accepted, equalTo(100));
        assertThat(sender.stats().dropped(DropReason.QUEUE_FULL), equalTo((long) attempts - 100));
        // Generous bound (GC pauses); a blocking offer would wait for the stuck send, i.e. forever.
        assertThat(Duration.ofNanos(maxOfferNanos[0]), lessThan(Duration.ofMillis(200)));

        producer.release();
        sender.close();
        assertThat(sender.stats().sent(), equalTo(101L));
    }

    @Test
    void senderThreadSurvivesAnythingThrownBySend() {
        var producer = new ThrowingProducer();
        var sender = start(producer, 100, CLOSE_TIMEOUT);

        for (String topic : List.of("kafka", "serialization", "runtime", "linkage", "interrupt", "ok")) {
            sender.offer(record(topic, "v"));
        }
        awaitUntil(() -> sender.stats().sent() == 1);
        // The InterruptException left the interrupt flag set; the thread must keep publishing anyway.
        sender.offer(record("ok", "again"));
        awaitUntil(() -> sender.stats().sent() == 2);

        SenderStats stats = sender.stats();
        assertThat(stats.failed("KafkaException"), equalTo(1L));
        assertThat(stats.failed("SerializationException"), equalTo(1L));
        assertThat(stats.failed("IllegalStateException"), equalTo(1L));
        assertThat(stats.failed("NoClassDefFoundError"), equalTo(1L));
        assertThat(stats.failed("InterruptException"), equalTo(1L));
        assertThat(sender.isSenderThreadAlive(), is(true));
    }

    @Test
    void closeDrainsTheQueueBeforeClosingTheProducer() throws Exception {
        var producer = gated(true);
        var sender = start(producer, 100, CLOSE_TIMEOUT);
        for (int i = 0; i < 21; i++) {
            sender.offer(record("t", "v" + i));
        }
        assertThat(producer.entered.await(WAIT.toSeconds(), TimeUnit.SECONDS), is(true));

        CompletableFuture<Void> closed = CompletableFuture.runAsync(sender::close);
        awaitUntil(sender::isClosing);
        assertThat(sender.offer(record("t", "late")), is(false));
        producer.release();
        closed.get(WAIT.toSeconds(), TimeUnit.SECONDS);

        SenderStats stats = sender.stats();
        assertThat(stats.sent(), equalTo(21L));
        assertThat(stats.dropped(DropReason.SHUTDOWN), equalTo(1L)); // only the late offer
        assertThat(stats.queueSize(), equalTo(0));
        assertThat(producer.closed(), is(true));
        assertThat(producer.closeCalls.get(), equalTo(1));
        assertThat(sender.isSenderThreadAlive(), is(false));
    }

    @Test
    void closeStopsAtTheTimeoutAndCountsQueuedRecordsAsDropped() {
        var producer = gated(true); // stuck in send() until interrupted, like a metadata wait
        var sender = start(producer, 100, Duration.ofMillis(200));
        offerAndAwaitStuck(sender, producer, 5);

        long elapsed = timeMillis(sender::close);

        assertThat(elapsed, lessThan(3_000L)); // 200 ms budget + CLOSE_MARGIN, with slack
        SenderStats stats = sender.stats();
        assertThat(stats.dropped(DropReason.SHUTDOWN), equalTo(5L));
        assertThat(stats.failed("InterruptException"), equalTo(1L));
        assertThat(stats.queueSize(), equalTo(0));
        assertThat(producer.closeCalls.get(), equalTo(1));
        assertThat(sender.isSenderThreadAlive(), is(false));
    }

    @Test
    void closeReturnsEvenIfSendIgnoresInterrupts() {
        var producer = gated(false);
        var sender = start(producer, 100, Duration.ofMillis(200));
        offerAndAwaitStuck(sender, producer, 5);

        long elapsed = timeMillis(sender::close);

        assertThat(elapsed, lessThan(3_000L));
        assertThat(sender.stats().dropped(DropReason.SHUTDOWN), equalTo(5L));
        assertThat(producer.closeCalls.get(), equalTo(1));

        // Once the stuck send returns, the abandoned thread notices the shutdown and exits.
        producer.release();
        awaitUntil(() -> !sender.isSenderThreadAlive());
        assertThat(sender.stats().failed("IllegalStateException"), equalTo(1L)); // MockProducer already closed
    }

    @Test
    void zeroCloseTimeoutDropsWhatIsQueued() {
        var producer = gated(true);
        var sender = start(producer, 100, Duration.ZERO);
        offerAndAwaitStuck(sender, producer, 3);

        long elapsed = timeMillis(sender::close);

        assertThat(elapsed, lessThan(3_000L));
        assertThat(sender.stats().dropped(DropReason.SHUTDOWN), equalTo(3L));
    }

    @Test
    void closeIsIdempotentAndOffersAfterCloseAreDropped() {
        var producer = gated(true);
        producer.release();
        var sender = start(producer, 10, CLOSE_TIMEOUT);

        sender.close();
        long secondClose = timeMillis(sender::close);

        assertThat(secondClose, lessThan(1_000L));
        assertThat(producer.closeCalls.get(), equalTo(1));
        assertThat(sender.offer(record("t", "v")), is(false));
        SenderStats stats = sender.stats();
        assertThat(stats.dropped(DropReason.SHUTDOWN), equalTo(1L));
        assertThat(stats.enqueued(), equalTo(0L));
    }

    @Test
    void statsReportQueueSize() {
        var producer = gated(true);
        var sender = start(producer, 10, CLOSE_TIMEOUT);
        offerAndAwaitStuck(sender, producer, 3);

        SenderStats stats = sender.stats();
        assertThat(stats.queueSize(), equalTo(3));
        assertThat(stats.enqueued(), equalTo(4L));
        assertThat(stats.dropped(DropReason.QUEUE_FULL), equalTo(0L));
        assertThat(stats.dropped(DropReason.SHUTDOWN), equalTo(0L));
    }

    @Test
    void offerOfNullIsRejectedWithoutThrowing() {
        var sender = start(new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer()),
                10, CLOSE_TIMEOUT);

        assertThat(sender.offer(null), is(false));
    }

    @Test
    void rejectsInvalidSettings() {
        var producer = new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());

        assertThrows(IllegalArgumentException.class, () -> KafkaSender.start(producer, 0, CLOSE_TIMEOUT));
        assertThrows(IllegalArgumentException.class,
                () -> KafkaSender.start(producer, 10, Duration.ofMillis(-1)));
    }

    // --- helpers -----------------------------------------------------------------------------------------

    private KafkaSender start(MockProducer<String, byte[]> producer, int capacity, Duration closeTimeout) {
        KafkaSender sender = KafkaSender.start(producer, capacity, closeTimeout);
        senders.add(sender);
        return sender;
    }

    private GatedProducer gated(boolean interruptible) {
        GatedProducer producer = new GatedProducer(interruptible);
        gates.add(producer);
        return producer;
    }

    /** Offers one record that the sender thread takes and gets stuck on, then {@code queued} more. */
    private static void offerAndAwaitStuck(KafkaSender sender, GatedProducer producer, int queued) {
        sender.offer(record("t", "stuck"));
        try {
            assertThat(producer.entered.await(WAIT.toSeconds(), TimeUnit.SECONDS), is(true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        for (int i = 0; i < queued; i++) {
            assertThat(sender.offer(record("t", "q" + i)), is(true));
        }
    }

    private static ProducerRecord<String, byte[]> record(String topic, String value) {
        return new ProducerRecord<>(topic, "key", value.getBytes(StandardCharsets.UTF_8));
    }

    private static long timeMillis(Runnable action) {
        long start = System.nanoTime();
        action.run();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("condition not met within " + WAIT);
            }
            Thread.onSpinWait();
            Thread.yield();
        }
    }

    /** Auto-completing producer whose {@code send} blocks until {@link #release()}. */
    private static final class GatedProducer extends MockProducer<String, byte[]> {

        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicInteger closeCalls = new AtomicInteger();
        private final CountDownLatch gate = new CountDownLatch(1);
        private final boolean interruptible;

        GatedProducer(boolean interruptible) {
            super(true, null, new StringSerializer(), new ByteArraySerializer());
            this.interruptible = interruptible;
        }

        void release() {
            gate.countDown();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback callback) {
            entered.countDown();
            if (interruptible) {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    throw new InterruptException(e); // what KafkaProducer does when interrupted
                }
            } else {
                awaitIgnoringInterrupts();
            }
            return super.send(record, callback);
        }

        private void awaitIgnoringInterrupts() {
            boolean interrupted = false;
            while (true) {
                try {
                    gate.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close(Duration timeout) {
            closeCalls.incrementAndGet();
            super.close(timeout);
        }
    }

    /** Auto-completing producer whose {@code send} throws, depending on the topic. */
    private static final class ThrowingProducer extends MockProducer<String, byte[]> {

        ThrowingProducer() {
            super(true, null, new StringSerializer(), new ByteArraySerializer());
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback callback) {
            switch (record.topic()) {
                case "kafka" -> throw new KafkaException("broken");
                case "serialization" -> throw new SerializationException("cannot serialize");
                case "runtime" -> throw new IllegalStateException("bad state");
                case "linkage" -> throw new NoClassDefFoundError("some/missing/Class");
                // Sets the interrupt flag on the sender thread, like KafkaProducer does.
                case "interrupt" -> throw new InterruptException(new InterruptedException());
                default -> {
                    return super.send(record, callback);
                }
            }
        }
    }
}
