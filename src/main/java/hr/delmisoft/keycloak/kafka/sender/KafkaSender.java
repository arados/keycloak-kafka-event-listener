package hr.delmisoft.keycloak.kafka.sender;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;

/**
 * Hands records from Keycloak request threads to Kafka without ever making those threads wait.
 *
 * <p>One instance per Keycloak node, owned by the listener factory. {@link #offer} (called from the
 * after-commit callback on request threads) only appends to a bounded queue: a full queue or a closed sender
 * drops the record and counts it. A single daemon thread ({@value #THREAD_NAME}) takes records and calls
 * {@link Producer#send}, which may block up to {@code max.block.ms} waiting for metadata — on that thread
 * only. Delivery outcomes are counted from the producer callback; failures are logged at most once per
 * {@link #FAILURE_LOG_INTERVAL} with a count of the suppressed ones. Payloads are never logged.
 *
 * <p>{@link #close()} stops accepting, lets the sender thread drain the queue until {@code close-timeout-ms}
 * runs out, then closes the producer with whatever is left of that budget. Records still queued afterwards
 * are counted as {@link DropReason#SHUTDOWN} drops.
 */
public final class KafkaSender implements AutoCloseable {

    static final String THREAD_NAME = "kafka-events-sender";

    static final Duration FAILURE_LOG_INTERVAL = Duration.ofSeconds(60);

    /**
     * How long {@link #close()} still waits for the sender thread after closing the producer (which makes a
     * {@code send()} still in progress fail fast), beyond {@code close-timeout-ms}.
     */
    static final Duration CLOSE_MARGIN = Duration.ofMillis(500);

    /** How often an idle sender thread re-checks whether {@link #close()} has started. */
    private static final long POLL_INTERVAL_MS = 50;

    private static final Logger LOG = Logger.getLogger(KafkaSender.class);

    /** Set once by {@link #close()}; carries the drain deadline so the sender thread sees both together. */
    private record Closing(long deadlineNanos) {
    }

    private final Producer<String, byte[]> producer;
    private final BlockingQueue<ProducerRecord<String, byte[]>> queue;
    private final int queueCapacity;
    private final Duration closeTimeout;
    private final LongSupplier nanoClock;
    private final FailureLogLimiter failureLog;
    private final Thread thread;
    private final AtomicReference<Closing> closing = new AtomicReference<>();

    private final LongAdder enqueued = new LongAdder();
    private final LongAdder sent = new LongAdder();
    private final Map<String, LongAdder> failed = new ConcurrentHashMap<>();
    private final Map<DropReason, LongAdder> dropped = new EnumMap<>(DropReason.class);

    private KafkaSender(Producer<String, byte[]> producer, int queueCapacity, Duration closeTimeout,
            Duration failureLogInterval, LongSupplier nanoClock) {
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }
        if (closeTimeout.isNegative()) {
            throw new IllegalArgumentException("closeTimeout must not be negative");
        }
        this.producer = Objects.requireNonNull(producer, "producer");
        this.queueCapacity = queueCapacity;
        this.closeTimeout = closeTimeout;
        this.nanoClock = nanoClock;
        // Linked, not array-backed: memory grows with the backlog instead of being preallocated for the bound.
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.failureLog = new FailureLogLimiter(failureLogInterval, nanoClock);
        for (DropReason reason : DropReason.values()) {
            // Fully populated here and never modified afterwards, so concurrent reads of the map are safe.
            dropped.put(reason, new LongAdder());
        }
        this.thread = new Thread(this::run, THREAD_NAME);
        thread.setDaemon(true);
        thread.setContextClassLoader(KafkaSender.class.getClassLoader());
        thread.setUncaughtExceptionHandler((t, e) -> LOG.errorf(e,
                "Kafka event sender thread died; Keycloak events are no longer published (they are dropped once"
                        + " the queue is full)"));
    }

    /**
     * Creates a sender and starts its thread.
     *
     * @param producer      the producer; the sender owns it from now on and closes it in {@link #close()}
     * @param queueCapacity maximum number of records waiting to be sent
     * @param closeTimeout  total budget of {@link #close()} for draining the queue and closing the producer
     */
    public static KafkaSender start(Producer<String, byte[]> producer, int queueCapacity, Duration closeTimeout) {
        return start(producer, queueCapacity, closeTimeout, FAILURE_LOG_INTERVAL, System::nanoTime);
    }

    static KafkaSender start(Producer<String, byte[]> producer, int queueCapacity, Duration closeTimeout,
            Duration failureLogInterval, LongSupplier nanoClock) {
        KafkaSender sender = new KafkaSender(producer, queueCapacity, closeTimeout, failureLogInterval, nanoClock);
        sender.thread.start();
        return sender;
    }

    /**
     * Queues a record for sending. Never blocks and never throws: when the queue is full or the sender is
     * closing, the record is dropped, counted, and {@code false} is returned.
     *
     * @return {@code true} if the record was queued
     */
    public boolean offer(ProducerRecord<String, byte[]> record) {
        if (record == null) {
            return false;
        }
        if (closing.get() != null) {
            drop(DropReason.SHUTDOWN);
            return false;
        }
        if (!queue.offer(record)) {
            drop(DropReason.QUEUE_FULL);
            return false;
        }
        // close() may have started between the check above and the insert, after its final sweep of the
        // queue. Take the record back so it is not stranded uncounted; if it is already gone, the sender
        // thread or close() took it and accounts for it.
        if (closing.get() != null && queue.remove(record)) {
            drop(DropReason.SHUTDOWN);
            return false;
        }
        enqueued.increment();
        return true;
    }

    /** Current counters. */
    public SenderStats stats() {
        Map<String, Long> failedSnapshot = new TreeMap<>();
        failed.forEach((reason, count) -> failedSnapshot.put(reason, count.sum()));
        Map<DropReason, Long> droppedSnapshot = new EnumMap<>(DropReason.class);
        dropped.forEach((reason, count) -> droppedSnapshot.put(reason, count.sum()));
        return new SenderStats(enqueued.sum(), sent.sum(), failedSnapshot, droppedSnapshot, queue.size(),
                queueCapacity);
    }

    /**
     * Stops accepting records, drains the queue for at most {@code closeTimeout}, then closes the producer
     * with the remaining budget. Returns within {@code closeTimeout} plus {@link #CLOSE_MARGIN}, even if
     * Kafka is unreachable. Records still queued are counted as {@link DropReason#SHUTDOWN} drops.
     * Idempotent; later calls return immediately.
     */
    @Override
    public void close() {
        Closing state = new Closing(nanoClock.getAsLong() + closeTimeout.toNanos());
        if (!closing.compareAndSet(null, state)) {
            return;
        }
        boolean interrupted = false;
        try {
            interrupted = join(remaining(state));
            if (thread.isAlive()) {
                // Out of budget while inside producer.send() (e.g. waiting max.block.ms for metadata).
                // KafkaProducer turns the interrupt into an InterruptException, which ends the wait.
                thread.interrupt();
            }
            closeProducer(remaining(state));
            interrupted |= join(CLOSE_MARGIN);
            if (thread.isAlive()) {
                LOG.warnf("Kafka event sender thread %s did not stop within close-timeout-ms; abandoning it",
                        THREAD_NAME);
            }
            int leftover = dropQueued();
            if (leftover > 0) {
                LOG.warnf("Dropped %d Keycloak events still queued for Kafka at shutdown: close-timeout-ms (%d ms)"
                        + " ran out before they could be sent", leftover, closeTimeout.toMillis());
            }
            SenderStats stats = stats();
            LOG.infof("Kafka event sender closed: enqueued=%d, sent=%d, failed=%d, dropped=%d", stats.enqueued(),
                    stats.sent(), stats.failedTotal(), stats.droppedTotal());
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** For tests: whether the sender thread is running. */
    boolean isSenderThreadAlive() {
        return thread.isAlive();
    }

    /** For tests: whether {@link #close()} has started. */
    boolean isClosing() {
        return closing.get() != null;
    }

    // --- sender thread -----------------------------------------------------------------------------------

    private void run() {
        while (true) {
            Closing state = closing.get();
            if (state != null && (queue.isEmpty() || nanoClock.getAsLong() - state.deadlineNanos() >= 0)) {
                return;
            }
            ProducerRecord<String, byte[]> record;
            try {
                record = queue.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Only close() interrupts this thread, once the drain budget is spent; stop then. Anything
                // else (e.g. KafkaProducer re-asserting an interrupt it caught) must not stop publishing,
                // and the interrupt status is already cleared by the exception.
                if (closing.get() != null) {
                    return;
                }
                continue;
            }
            if (record != null) {
                send(record);
            }
        }
    }

    private void send(ProducerRecord<String, byte[]> record) {
        String topic = record.topic();
        try {
            producer.send(record, (metadata, exception) -> onCompletion(topic, exception));
        } catch (RuntimeException | LinkageError e) {
            // KafkaException (incl. TimeoutException, SerializationException, InterruptException), producer
            // closed, a broken interceptor or partitioner... Count it and keep going: this thread must not die.
            onFailure(topic, e);
        }
    }

    /** Producer callback; runs on Kafka's I/O thread, so it only counts and (rarely) logs. */
    private void onCompletion(String topic, Exception exception) {
        if (exception == null) {
            sent.increment();
        } else {
            onFailure(topic, exception);
        }
    }

    private void onFailure(String topic, Throwable error) {
        String reason = reason(error);
        failed.computeIfAbsent(reason, r -> new LongAdder()).increment();
        long suppressed = failureLog.tryAcquire();
        if (suppressed == FailureLogLimiter.SUPPRESSED) {
            return;
        }
        // Topic and exception message only: Kafka's messages never contain record keys or values.
        String summary = suppressed > 0
                ? " (" + suppressed + " more failures since the last report)"
                : "";
        LOG.warnf("Failed to publish a Keycloak event to Kafka topic '%s' (%s: %s)%s; further failures are"
                + " logged at most once every %d s", topic, reason, error.getMessage(), summary,
                FAILURE_LOG_INTERVAL.toSeconds());
        LOG.debugf(error, "Kafka publish failure on topic '%s'", topic);
    }

    private static String reason(Throwable error) {
        String name = error.getClass().getSimpleName();
        return name.isEmpty() ? error.getClass().getName() : name;
    }

    // --- shutdown ----------------------------------------------------------------------------------------

    private Duration remaining(Closing state) {
        return Duration.ofNanos(Math.max(0, state.deadlineNanos() - nanoClock.getAsLong()));
    }

    /** Waits for the sender thread; returns whether the caller was interrupted (it then stops waiting). */
    private boolean join(Duration timeout) {
        try {
            thread.join(timeout);
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    private void closeProducer(Duration timeout) {
        try {
            producer.close(timeout);
        } catch (RuntimeException | LinkageError e) {
            LOG.warnf("Closing the Kafka producer failed: %s: %s", reason(e), e.getMessage());
        }
    }

    private int dropQueued() {
        int count = 0;
        while (queue.poll() != null) {
            count++;
        }
        dropped.get(DropReason.SHUTDOWN).add(count);
        return count;
    }

    private void drop(DropReason reason) {
        dropped.get(reason).increment();
    }
}
