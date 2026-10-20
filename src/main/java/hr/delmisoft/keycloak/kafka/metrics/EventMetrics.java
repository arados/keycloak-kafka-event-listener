package hr.delmisoft.keycloak.kafka.metrics;

import java.util.function.IntSupplier;

/**
 * What the listener reports about its events. Every call is cheap and never throws; {@link #NOOP} ignores
 * everything (tests, listener disabled). The Micrometer implementation is {@link MicrometerEventMetrics}.
 *
 * <p>Tag values: {@code kind} is {@code user}, {@code admin} or {@value #KIND_UNKNOWN}; {@code topic} is the
 * resolved Kafka topic; failure {@code reason} is the exception's simple class name; drop {@code reason} is one
 * of {@code queue_full}, {@code shutdown}, {@value #DROP_FILTERED} or {@value #DROP_ERROR}.
 */
public interface EventMetrics {

    /** {@code kind} of a record without a {@code kc-kind} header. */
    String KIND_UNKNOWN = "unknown";

    /** Drop reason: the event matched an include/exclude filter and was not published. */
    String DROP_FILTERED = "filtered";

    /** Drop reason: the listener failed while preparing the event or handing it over after commit. */
    String DROP_ERROR = "error";

    /** Ignores everything. */
    EventMetrics NOOP = new EventMetrics() {
        @Override
        public void enqueued(String kind) {
            // no-op
        }

        @Override
        public void sent(String kind, String topic) {
            // no-op
        }

        @Override
        public void failed(String kind, String reason) {
            // no-op
        }

        @Override
        public void dropped(String kind, String reason) {
            // no-op
        }

        @Override
        public void deadLettered(String kind) {
            // no-op
        }

        @Override
        public void queue(IntSupplier size, int capacity) {
            // no-op
        }
    };

    /** An event was accepted for sending (async queue or outbox). */
    void enqueued(String kind);

    /** Kafka acknowledged an event. */
    void sent(String kind, String topic);

    /** A send attempt failed; in outbox mode the event is retried later. */
    void failed(String kind, String reason);

    /** An event was not published at all. */
    void dropped(String kind, String reason);

    /** Outbox mode: an event was given up on after a permanent error or too many attempts. */
    void deadLettered(String kind);

    /** Async mode: exposes the in-memory queue's current size and its bound. */
    void queue(IntSupplier size, int capacity);
}
