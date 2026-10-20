package hr.delmisoft.keycloak.kafka.outbox;

import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.keycloak.util.JsonSerialization;

/**
 * Converts the Kafka record {@code onEvent} built into an outbox row and back, so the drainer publishes exactly
 * what async mode would have (topic and key resolved at event time, same headers and bytes).
 *
 * <ul>
 *   <li>payload: the record value, the envelope JSON, as text (it is UTF-8 JSON, so the round trip is
 *       lossless);</li>
 *   <li>metadata: JSON {@code {"topic":"…","key":"…","headers":{"name":"value",…}}}; header values are the
 *       UTF-8 strings {@link EventRecordFactory} writes.</li>
 * </ul>
 * Values are checked against the outbox column widths before anything touches the database, so a bad event never
 * poisons the Keycloak transaction with a constraint violation.
 */
public final class OutboxCodec {

    private static final String TOPIC = "topic";
    private static final String KEY = "key";
    private static final String HEADERS = "headers";

    /** {@code OUTBOX_ENTRY.REALM_ID} is not nullable; Keycloak events always carry a realm. */
    static final String NO_REALM = "_";

    private OutboxCodec() {
    }

    /**
     * @throws IllegalArgumentException if a value does not fit the outbox columns
     */
    public static OutboxEntry encode(Envelope envelope, ProducerRecord<String, byte[]> record) {
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(record, "record");
        String realmId = envelope.kcrealmid() == null ? NO_REALM : envelope.kcrealmid();
        String eventId = envelope.id() == null ? UUID.randomUUID().toString() : envelope.id();
        String type = envelope.type();
        checkLength("realm id", realmId, KeycloakOutboxRepository.MAX_REALM_ID);
        checkLength("event id", eventId, KeycloakOutboxRepository.MAX_CORRELATION_ID);
        checkLength("event type", type, KeycloakOutboxRepository.MAX_ENTRY_TYPE);

        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put(TOPIC, record.topic());
        metadata.put(KEY, record.key());
        metadata.put(HEADERS, headers);
        return new OutboxEntry(realmId, eventId, type, EventRecordFactory.kindOf(record),
                new String(record.value(), StandardCharsets.UTF_8), json(metadata));
    }

    /**
     * @throws IllegalArgumentException if the row's metadata is not what {@link #encode} writes
     */
    public static ProducerRecord<String, byte[]> decode(OutboxRow row) {
        Map<?, ?> metadata;
        try {
            metadata = JsonSerialization.readValue(row.metadata(), Map.class);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("unreadable outbox metadata", e);
        }
        if (metadata == null || !(metadata.get(TOPIC) instanceof String topic) || topic.isEmpty()
                || row.payload() == null) {
            throw new IllegalArgumentException("outbox metadata without a topic, or no payload");
        }
        RecordHeaders headers = new RecordHeaders();
        if (metadata.get(HEADERS) instanceof Map<?, ?> map) {
            map.forEach((name, value) -> headers.add(String.valueOf(name),
                    value == null ? null : String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        }
        Object key = metadata.get(KEY);
        return new ProducerRecord<>(topic, null, key == null ? null : String.valueOf(key),
                row.payload().getBytes(StandardCharsets.UTF_8), headers);
    }

    private static void checkLength(String what, String value, int max) {
        if (value == null || value.isEmpty() || value.length() > max) {
            throw new IllegalArgumentException(what + " is empty or longer than " + max + " characters");
        }
    }

    private static String json(Map<String, Object> value) {
        try {
            return JsonSerialization.writeValueAsString(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
