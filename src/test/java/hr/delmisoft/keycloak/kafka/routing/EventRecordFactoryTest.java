package hr.delmisoft.keycloak.kafka.routing;

import static hr.delmisoft.keycloak.kafka.routing.TestEvents.REALM_ID;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.USER_ID;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.adminEvent;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.envelope;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.userEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfigException;
import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.ResourceType;

class EventRecordFactoryTest {

    private final EventRecordFactory factory = EventRecordFactory.from(TestEvents.config(
            "kc.{realmName}.events", "kc.admin", Map.of(EventType.LOGIN_ERROR, "kc.security"),
            Set.of(), Set.of(), Set.of(), Set.of()));

    @Test
    void userEventRecord() {
        Envelope envelope = envelope(userEvent(EventType.LOGIN));
        byte[] value = EnvelopeMapper.toJsonBytes(envelope);

        ProducerRecord<String, byte[]> record = factory.create(envelope, value);

        assertThat(record.topic(), is("kc.kafka-demo.events"));
        assertThat(record.key(), is(REALM_ID + ":" + USER_ID));
        assertThat(record.value(), sameInstance(value));
        assertThat(record.partition(), nullValue());
        assertThat(record.timestamp(), nullValue());
        assertThat(headerNames(record), contains(
                "content-type", "kc-kind", "kc-event-type", "kc-realm-id", "kc-schema-version"));
        assertThat(header(record, "content-type"), is("application/cloudevents+json; charset=UTF-8"));
        assertThat(header(record, "kc-kind"), is("user"));
        assertThat(header(record, "kc-event-type"), is("org.keycloak.event.LOGIN"));
        assertThat(header(record, "kc-realm-id"), is(REALM_ID));
        assertThat(header(record, "kc-schema-version"), is("1"));
    }

    @Test
    void overriddenUserEventRecord() {
        Event event = userEvent(EventType.LOGIN_ERROR);
        event.setUserId(null);
        event.setSessionId(null);
        Envelope envelope = envelope(event);

        ProducerRecord<String, byte[]> record = factory.create(envelope, EnvelopeMapper.toJsonBytes(envelope));

        assertThat(record.topic(), is("kc.security"));
        assertThat(record.key(), is(REALM_ID));
        assertThat(header(record, "kc-event-type"), is("org.keycloak.event.LOGIN_ERROR"));
    }

    @Test
    void adminEventRecord() {
        Envelope envelope = envelope(adminEvent(ResourceType.USER, "users/u1/role-mappings/realm"));

        ProducerRecord<String, byte[]> record = factory.create(envelope, EnvelopeMapper.toJsonBytes(envelope));

        assertThat(record.topic(), is("kc.admin"));
        assertThat(record.key(), is(REALM_ID + ":users/u1"));
        assertThat(header(record, "content-type"), is(Envelope.CONTENT_TYPE));
        assertThat(header(record, "kc-kind"), is("admin"));
        assertThat(header(record, "kc-event-type"), is("org.keycloak.admin.USER.UPDATE"));
        assertThat(header(record, "kc-realm-id"), is(REALM_ID));
        assertThat(header(record, "kc-schema-version"), is(String.valueOf(Envelope.SCHEMA_VERSION)));
    }

    @Test
    void headersMatchTheEnvelopeAttributes() {
        Envelope envelope = envelope(userEvent(EventType.CODE_TO_TOKEN));

        ProducerRecord<String, byte[]> record = factory.create(envelope, new byte[0]);

        assertThat(header(record, "kc-kind"), is(envelope.kckind().value()));
        assertThat(header(record, "kc-event-type"), is(envelope.type()));
        assertThat(header(record, "kc-realm-id"), is(envelope.kcrealmid()));
        assertThat(header(record, "kc-schema-version"), is(String.valueOf(envelope.kcschemaversion())));
    }

    @Test
    void realmIdHeaderIsOmittedWhenAbsent() {
        Event event = userEvent(EventType.LOGIN);
        event.setRealmId(null);
        Envelope envelope = envelope(event);

        ProducerRecord<String, byte[]> record = factory.create(envelope, new byte[0]);

        assertThat(record.headers().lastHeader("kc-realm-id"), nullValue());
    }

    @Test
    void headerValuesAreUtf8() {
        // A custom resource type from an extension ends up in the CloudEvents type and the header.
        AdminEvent event = adminEvent(null, "x/y");
        event.setResourceTypeAsString("ŽIVOT");

        ProducerRecord<String, byte[]> record = factory.create(envelope(event), new byte[0]);

        assertThat(record.headers().lastHeader("kc-event-type").value(),
                is("org.keycloak.admin.ŽIVOT.UPDATE".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void recordsDoNotShareHeaderArrays() {
        Envelope envelope = envelope(userEvent(EventType.LOGIN));

        ProducerRecord<String, byte[]> first = factory.create(envelope, new byte[0]);
        ProducerRecord<String, byte[]> second = factory.create(envelope, new byte[0]);

        assertThat(first.headers().lastHeader("content-type").value()
                == second.headers().lastHeader("content-type").value(), is(false));
    }

    @Test
    void invalidTopicConfigFailsAtConstruction() {
        assertThrows(KafkaEventsConfigException.class, () -> EventRecordFactory.from(TestEvents.config(
                "bad topic", "ok", Map.of(), Set.of(), Set.of(), Set.of(), Set.of())));
    }

    @Test
    void nullArgumentsAreRejected() {
        Envelope envelope = envelope(userEvent(EventType.LOGIN));

        assertThrows(NullPointerException.class, () -> factory.create(null, new byte[0]));
        assertThrows(NullPointerException.class, () -> factory.create(envelope, null));
    }

    private static List<String> headerNames(ProducerRecord<String, byte[]> record) {
        List<String> names = new ArrayList<>();
        for (Header header : record.headers()) {
            names.add(header.key());
        }
        return names;
    }

    private static String header(ProducerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
