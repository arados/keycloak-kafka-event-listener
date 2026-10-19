package hr.delmisoft.keycloak.kafka.sender;

import java.util.Map;

/**
 * Point-in-time snapshot of {@link KafkaSender} counters. Counters are cumulative since the sender started;
 * the individual values are read one after another, so they are not an atomic cut across all counters.
 *
 * @param enqueued      records accepted by {@link KafkaSender#offer}
 * @param sent          records Kafka acknowledged (producer callback without exception)
 * @param failed        records that failed, by exception simple name (e.g. {@code TimeoutException}), whether
 *                      {@code send()} threw or the producer callback reported the error
 * @param dropped       records dropped before reaching the producer, by reason; contains every
 *                      {@link DropReason}, zero when none
 * @param queueSize     records currently waiting in the queue
 * @param queueCapacity the queue's bound ({@code queue-capacity})
 */
public record SenderStats(
        long enqueued,
        long sent,
        Map<String, Long> failed,
        Map<DropReason, Long> dropped,
        int queueSize,
        int queueCapacity) {

    public SenderStats {
        failed = Map.copyOf(failed);
        dropped = Map.copyOf(dropped);
    }

    /** Failed records for one reason (exception simple name); zero when none. */
    public long failed(String reason) {
        return failed.getOrDefault(reason, 0L);
    }

    /** Dropped records for one reason; zero when none. */
    public long dropped(DropReason reason) {
        return dropped.getOrDefault(reason, 0L);
    }

    public long failedTotal() {
        return failed.values().stream().mapToLong(Long::longValue).sum();
    }

    public long droppedTotal() {
        return dropped.values().stream().mapToLong(Long::longValue).sum();
    }
}
