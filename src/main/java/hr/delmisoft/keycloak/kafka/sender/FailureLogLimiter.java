package hr.delmisoft.keycloak.kafka.sender;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Decides which delivery failures get a log line: the first one immediately, then at most one per interval,
 * reporting how many were suppressed in between. Thread-safe (failures are reported from the sender thread
 * and from Kafka's I/O thread); only called on failures, so the lock is never on a hot path.
 */
final class FailureLogLimiter {

    /** {@link #tryAcquire()} result: don't log this failure. */
    static final long SUPPRESSED = -1;

    private final long intervalNanos;
    private final LongSupplier nanoClock;

    private boolean loggedBefore;
    private long lastLogNanos;
    private long suppressedSinceLastLog;

    FailureLogLimiter(Duration interval, LongSupplier nanoClock) {
        this.intervalNanos = interval.toNanos();
        this.nanoClock = nanoClock;
    }

    /**
     * Registers one failure.
     *
     * @return {@link #SUPPRESSED} if this failure must not be logged, otherwise the number of failures
     *         suppressed since the previous log line (0 for the first one)
     */
    synchronized long tryAcquire() {
        long now = nanoClock.getAsLong();
        if (loggedBefore && now - lastLogNanos < intervalNanos) {
            suppressedSinceLastLog++;
            return SUPPRESSED;
        }
        long suppressed = suppressedSinceLastLog;
        loggedBefore = true;
        lastLogNanos = now;
        suppressedSinceLastLog = 0;
        return suppressed;
    }
}
