package hr.delmisoft.keycloak.kafka.outbox;

import java.time.Instant;

/**
 * A pending row locked for delivery.
 *
 * @param id        the row id
 * @param eventId   the envelope id
 * @param payload   the envelope JSON
 * @param metadata  topic, key and headers, see {@link OutboxCodec}
 * @param attempts  failed attempts counted so far
 * @param createdAt when the event was written
 */
public record OutboxRow(String id, String eventId, String payload, String metadata, int attempts, Instant createdAt) {
}
