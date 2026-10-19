package hr.delmisoft.keycloak.kafka.sender;

import static hr.delmisoft.keycloak.kafka.sender.FailureLogLimiter.SUPPRESSED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FailureLogLimiterTest {

    private static final Duration INTERVAL = Duration.ofSeconds(60);

    /** Arbitrary start, including negative: System.nanoTime() has no fixed origin. */
    private final AtomicLong clock = new AtomicLong(-5_000L);

    private final FailureLogLimiter limiter = new FailureLogLimiter(INTERVAL, clock::get);

    @Test
    void firstFailureIsLoggedImmediately() {
        assertThat(limiter.tryAcquire(), equalTo(0L));
    }

    @Test
    void failuresWithinTheIntervalAreSuppressedAndReportedWithTheNextLogLine() {
        limiter.tryAcquire();

        advance(Duration.ofSeconds(1));
        assertThat(limiter.tryAcquire(), equalTo(SUPPRESSED));
        assertThat(limiter.tryAcquire(), equalTo(SUPPRESSED));
        advance(Duration.ofSeconds(58));
        assertThat(limiter.tryAcquire(), equalTo(SUPPRESSED));

        advance(Duration.ofSeconds(1)); // exactly one interval after the first log line
        assertThat(limiter.tryAcquire(), equalTo(3L));
        assertThat(limiter.tryAcquire(), equalTo(SUPPRESSED));

        advance(INTERVAL);
        assertThat(limiter.tryAcquire(), equalTo(1L));
    }

    @Test
    void afterAQuietPeriodTheNextFailureLogsWithNothingSuppressed() {
        limiter.tryAcquire();

        advance(INTERVAL.multipliedBy(10));

        assertThat(limiter.tryAcquire(), equalTo(0L));
    }

    private void advance(Duration duration) {
        clock.addAndGet(duration.toNanos());
    }
}
