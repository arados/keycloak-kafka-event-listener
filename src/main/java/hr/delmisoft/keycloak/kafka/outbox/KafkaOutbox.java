package hr.delmisoft.keycloak.kafka.outbox;

import hr.delmisoft.keycloak.kafka.config.OutboxOptions;
import hr.delmisoft.keycloak.kafka.metrics.MicrometerEventMetrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.Producer;
import org.jboss.logging.Logger;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.utils.PostMigrationEvent;
import org.keycloak.provider.ProviderEventListener;
import org.keycloak.services.scheduled.ClusterAwareScheduledTaskRunner;
import org.keycloak.timer.TimerProvider;

/**
 * {@code delivery-mode=outbox} at runtime: the {@link OutboxWriter} the providers use, and the
 * {@link KafkaOutboxDrainer} scheduled with Keycloak's {@link TimerProvider} on every node, wrapped in Keycloak's
 * {@code ClusterAwareScheduledTaskRunner} (internal, like the outbox classes) so one node drains per interval.
 * Scheduling waits for Keycloak's {@link PostMigrationEvent}: factories are initialised before Keycloak creates or
 * migrates the database schema, so an earlier run could find no {@code OUTBOX_ENTRY} table. The timer only hands
 * each run to this listener's own thread ({@value #THREAD_NAME}), skipping a tick while a run is still going: a
 * run may wait for Kafka for up to {@code delivery.timeout.ms}, and Keycloak's timer thread is shared by all its
 * scheduled tasks. Registers the outbox gauges. Owned by the listener factory; {@link #close()} cancels the task and closes the producer.
 */
public final class KafkaOutbox implements OutboxRuntime {

    static final String THREAD_NAME = "kafka-events-outbox";

    private static final Logger LOG = Logger.getLogger(KafkaOutbox.class);

    private final KeycloakSessionFactory sessionFactory;
    private final Producer<String, byte[]> producer;
    private final Duration closeTimeout;
    private final OutboxWriter writer;
    private final KafkaOutboxDrainer drainer;
    private final long intervalMillis;
    private final ProviderEventListener scheduleAfterMigration;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, THREAD_NAME);
        thread.setDaemon(true);
        thread.setContextClassLoader(KafkaOutbox.class.getClassLoader());
        return thread;
    });
    private final AtomicBoolean running = new AtomicBoolean();
    private final Object lock = new Object();
    private boolean scheduled;
    private boolean closed;

    private KafkaOutbox(KeycloakSessionFactory sessionFactory, Producer<String, byte[]> producer,
            Duration closeTimeout, OutboxWriter writer, KafkaOutboxDrainer drainer, long intervalMillis) {
        this.sessionFactory = sessionFactory;
        this.producer = producer;
        this.closeTimeout = closeTimeout;
        this.writer = writer;
        this.drainer = drainer;
        this.intervalMillis = intervalMillis;
        this.scheduleAfterMigration = event -> {
            if (event instanceof PostMigrationEvent) {
                schedule();
            }
        };
    }

    /**
     * Schedules the drainer and registers the gauges.
     *
     * @param producer        owned from now on, closed by {@link #close()}
     * @param deliveryTimeout the producer's {@code delivery.timeout.ms}
     */
    public static KafkaOutbox start(KeycloakSessionFactory sessionFactory, Producer<String, byte[]> producer,
            OutboxOptions options, Duration deliveryTimeout, Duration closeTimeout, MicrometerEventMetrics metrics) {
        Objects.requireNonNull(sessionFactory, "sessionFactory");
        KafkaOutboxDrainer drainer = new KafkaOutboxDrainer(KeycloakOutboxRepository::new, producer, options, deliveryTimeout,
                metrics, Clock.systemUTC());
        long interval = options.drainInterval().toMillis();
        KafkaOutbox outbox = new KafkaOutbox(sessionFactory, producer, closeTimeout,
                new OutboxWriter(KeycloakOutboxRepository::new, metrics), drainer, interval);
        sessionFactory.register(outbox.scheduleAfterMigration);
        String prefix = MicrometerEventMetrics.PREFIX + "outbox.";
        metrics.gauge(prefix + "pending", "Outbox rows waiting for delivery (as of this node's last drain)",
                () -> drainer.stats().pending());
        metrics.gauge(prefix + "dead.letters", "Outbox rows given up on and not purged yet",
                () -> drainer.stats().deadLetters());
        metrics.gauge(prefix + "oldest.pending.age.seconds", "Age of the oldest undelivered outbox row",
                () -> {
                    Instant oldest = drainer.stats().oldestPendingCreatedAt();
                    return oldest == null ? 0 : Math.max(0, Duration.between(oldest, Instant.now()).toSeconds());
                });
        LOG.infof("Kafka event listener outbox drainer will run every %d ms once Keycloak has started (batch size"
                + " %d, max attempts %d)", interval, options.batchSize(), options.maxAttempts());
        return outbox;
    }

    private void schedule() {
        synchronized (lock) {
            scheduleLocked();
        }
    }

    private void scheduleLocked() {
        if (scheduled || closed) {
            return;
        }
        Runnable run = new ClusterAwareScheduledTaskRunner(sessionFactory, drainer, intervalMillis);
        try (KeycloakSession session = sessionFactory.create()) {
            session.getProvider(TimerProvider.class).schedule(() -> handOver(run), intervalMillis,
                    KafkaOutboxDrainer.TASK_NAME);
        }
        scheduled = true;
        LOG.debugf("Kafka event listener outbox drainer scheduled");
    }

    /** On Keycloak's timer thread: start a run on our thread unless one is still in progress. */
    private void handOver(Runnable run) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    run.run(); // ScheduledTaskRunner logs and swallows failures
                } finally {
                    running.set(false);
                }
            });
        } catch (RejectedExecutionException closing) {
            running.set(false);
        }
    }

    @Override
    public OutboxWriter writer() {
        return writer;
    }

    /** Cancels the drainer and closes the producer; undelivered rows stay in the outbox. Idempotent. */
    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        sessionFactory.unregister(scheduleAfterMigration);
        if (scheduled) {
            try (KeycloakSession session = sessionFactory.create()) {
                session.getProvider(TimerProvider.class).cancelTask(KafkaOutboxDrainer.TASK_NAME);
            } catch (RuntimeException e) {
                LOG.debugf(e, "Could not cancel the outbox drainer task (Keycloak is shutting down)");
            }
        }
        executor.shutdownNow(); // interrupts a run waiting for Kafka; its rows stay due
        try {
            executor.awaitTermination(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            producer.close(closeTimeout);
        } catch (RuntimeException e) {
            LOG.warnf("Closing the Kafka producer failed: %s: %s", e.getClass().getSimpleName(), e.getMessage());
        }
        OutboxStats stats = drainer.stats();
        LOG.infof("Kafka event listener outbox closed: %d rows pending, %d dead-lettered (as of the last drain on"
                + " this node); pending rows are published after the next start", stats.pending(), stats.deadLetters());
    }
}
