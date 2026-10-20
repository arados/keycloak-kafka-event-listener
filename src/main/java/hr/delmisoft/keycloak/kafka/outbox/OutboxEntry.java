package hr.delmisoft.keycloak.kafka.outbox;

/**
 * A row to insert into the outbox.
 *
 * @param realmId  the event's realm (also used as the row's owner, so a realm's rows can be found and removed)
 * @param eventId  the envelope {@code id}: the deduplication key, and what consumers deduplicate on
 * @param type     the CloudEvents {@code type}, e.g. {@code org.keycloak.event.LOGIN}
 * @param kind     {@code user} or {@code admin} (metrics tag; not stored separately)
 * @param payload  the envelope JSON (the Kafka record value)
 * @param metadata topic, key and headers of the Kafka record, see {@link OutboxCodec}
 */
public record OutboxEntry(String realmId, String eventId, String type, String kind, String payload, String metadata) {
}
