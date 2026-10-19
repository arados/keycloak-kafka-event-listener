package hr.delmisoft.keycloak.kafka.sender;

/** Why {@link KafkaSender} dropped a record without handing it to the producer. */
public enum DropReason {

    /** The bounded queue was full ({@code queue-capacity}): Kafka is slower than the event rate, or down. */
    QUEUE_FULL("queue_full"),

    /**
     * The sender was shutting down: the record was offered after {@link KafkaSender#close()} started, or was
     * still queued when {@code close-timeout-ms} ran out.
     */
    SHUTDOWN("shutdown");

    private final String tag;

    DropReason(String tag) {
        this.tag = tag;
    }

    /** Stable lower-case name for metric tags and logs, e.g. {@code queue_full}. */
    public String tag() {
        return tag;
    }
}
