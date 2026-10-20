package hr.delmisoft.keycloak.kafka.outbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeOptions;
import hr.delmisoft.keycloak.kafka.routing.EventRecordFactory;
import hr.delmisoft.keycloak.kafka.routing.KeyStrategy;
import hr.delmisoft.keycloak.kafka.routing.TopicRouter;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeContext;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;

class OutboxCodecTest {

    private final EventRecordFactory records =
            new EventRecordFactory(new TopicRouter("keycloak.events", "keycloak.admin-events", Map.of()),
                    new KeyStrategy());

    @Test
    void roundTripReproducesTheRecordExactly() {
        Envelope envelope = envelope("event-1", "realm-1", "Ünïcødé ✓");
        ProducerRecord<String, byte[]> record = records.create(envelope, EnvelopeMapper.toJsonBytes(envelope));

        OutboxEntry entry = OutboxCodec.encode(envelope, record);
        ProducerRecord<String, byte[]> decoded = OutboxCodec.decode(row(entry));

        assertThat(entry.realmId(), equalTo("realm-1"));
        assertThat(entry.eventId(), equalTo("event-1"));
        assertThat(entry.type(), equalTo("org.keycloak.event.LOGIN"));
        assertThat(entry.kind(), equalTo("user"));
        assertThat(decoded.topic(), equalTo(record.topic()));
        assertThat(decoded.key(), equalTo(record.key()));
        assertThat(decoded.value(), equalTo(record.value()));
        assertThat(decoded.partition(), nullValue());
        for (Header header : record.headers()) {
            assertThat(new String(decoded.headers().lastHeader(header.key()).value(), StandardCharsets.UTF_8),
                    equalTo(new String(header.value(), StandardCharsets.UTF_8)));
        }
        assertThat(decoded.headers().toArray().length, equalTo(record.headers().toArray().length));
    }

    @Test
    void valuesThatDoNotFitTheOutboxColumnsAreRejectedBeforeAnyDatabaseWork() {
        Envelope longRealm = envelope("event-1", "r".repeat(37), "u");
        ProducerRecord<String, byte[]> record = records.create(longRealm, EnvelopeMapper.toJsonBytes(longRealm));

        var error = assertThrows(IllegalArgumentException.class, () -> OutboxCodec.encode(longRealm, record));

        assertThat(error.getMessage(), containsString("realm id"));
    }

    @Test
    void unreadableMetadataIsReported() {
        OutboxRow broken = new OutboxRow("row-1", "event-1", "{}", "not json", 0, null);
        OutboxRow noTopic = new OutboxRow("row-1", "event-1", "{}", "{\"key\":\"k\"}", 0, null);

        assertThrows(IllegalArgumentException.class, () -> OutboxCodec.decode(broken));
        assertThrows(IllegalArgumentException.class, () -> OutboxCodec.decode(noTopic));
    }

    static OutboxRow row(OutboxEntry entry) {
        return new OutboxRow("row-1", entry.eventId(), entry.payload(), entry.metadata(), 0, null);
    }

    static Envelope envelope(String id, String realmId, String username) {
        Event event = new Event();
        event.setId(id);
        event.setType(EventType.LOGIN);
        event.setTime(1_760_877_296_789L);
        event.setRealmId(realmId);
        event.setRealmName("demo");
        event.setUserId("user-1");
        event.setDetails(Map.of("username", username));
        return new EnvelopeMapper(EnvelopeOptions.DEFAULTS)
                .map(event, new EnvelopeContext("urn:keycloak:realm:" + realmId, "demo"));
    }
}
