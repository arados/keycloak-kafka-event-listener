package hr.delmisoft.keycloak.kafka;

import hr.delmisoft.keycloak.kafka.sender.FailureLogLimiter;
import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/**
 * Counts events the listener could not hand to the sender because something failed while preparing them in
 * {@code onEvent} or handing them over after commit, and logs those failures at most once per
 * {@link #LOG_INTERVAL} with the number suppressed in between. Never logs event contents: only the event
 * kind and type and the exception class and message. One instance per factory; thread-safe.
 */
final class ListenerFailures {

    static final Duration LOG_INTERVAL = Duration.ofSeconds(60);

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

    private final LongAdder count = new LongAdder();
    private final FailureLogLimiter limiter;

    ListenerFailures() {
        this(System::nanoTime);
    }

    ListenerFailures(LongSupplier nanoClock) {
        this.limiter = new FailureLogLimiter(LOG_INTERVAL, nanoClock);
    }

    /**
     * Records one failure.
     *
     * @param what  what was being done, without event data, e.g. {@code "preparing user event LOGIN"}
     * @param error the failure
     */
    void record(String what, Throwable error) {
        count.increment();
        long suppressed = limiter.tryAcquire();
        if (suppressed == FailureLogLimiter.SUPPRESSED) {
            return;
        }
        String summary = suppressed > 0 ? " (" + suppressed + " more failures since the last report)" : "";
        LOG.warnf("Kafka event listener failed while %s (%s: %s); the event is not published%s. The Keycloak"
                + " request is not affected. Further failures are logged at most once every %d s", what,
                error.getClass().getName(), error.getMessage(), summary, LOG_INTERVAL.toSeconds());
        LOG.debugf(error, "Kafka event listener failure while %s", what);
    }

    /** Failures recorded since startup. */
    long count() {
        return count.sum();
    }
}
