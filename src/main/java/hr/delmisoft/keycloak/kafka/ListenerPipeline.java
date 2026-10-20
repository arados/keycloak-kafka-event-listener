package hr.delmisoft.keycloak.kafka;

import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.metrics.EventMetrics;
import hr.delmisoft.keycloak.kafka.routing.EventFilter;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import java.util.Objects;

/**
 * The server-wide collaborators every per-session {@link KafkaEventListenerProvider} uses. Built once by the
 * factory; all parts are immutable or thread-safe.
 *
 * @param filter        which events are published at all
 * @param mapper        event → envelope
 * @param recordFactory envelope → Kafka record (topic, key, headers)
 * @param sink          where committed records go (the sender's queue)
 * @param failures      counter and rate-limited log for events that could not be prepared or handed over
 * @param metrics       where filtered events are counted
 */
record ListenerPipeline(
        EventFilter filter,
        EnvelopeMapper mapper,
        EventRecordFactory recordFactory,
        RecordSink sink,
        ListenerFailures failures,
        EventMetrics metrics) {

    ListenerPipeline {
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(mapper, "mapper");
        Objects.requireNonNull(recordFactory, "recordFactory");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(failures, "failures");
        Objects.requireNonNull(metrics, "metrics");
    }
}
