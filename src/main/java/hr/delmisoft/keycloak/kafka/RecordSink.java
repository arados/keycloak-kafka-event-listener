package hr.delmisoft.keycloak.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Where committed records go: {@link hr.delmisoft.keycloak.kafka.sender.KafkaSender#offer} in production, a
 * list in unit tests.
 */
@FunctionalInterface
interface RecordSink {

    /**
     * Hands a record over for sending. Must never block and never throw; a record that cannot be accepted
     * is dropped and counted by the sink.
     *
     * @return whether the record was accepted
     */
    boolean offer(ProducerRecord<String, byte[]> record);
}
