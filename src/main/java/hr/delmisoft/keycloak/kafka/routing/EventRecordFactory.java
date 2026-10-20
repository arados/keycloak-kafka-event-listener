package hr.delmisoft.keycloak.kafka.routing;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Turns an {@link Envelope} and its serialized JSON into the {@link ProducerRecord} we send: topic from
 * {@link TopicRouter}, key from {@link KeyStrategy}, value = the JSON bytes, plus headers that let consumers
 * route and filter without parsing the value. Header values are UTF-8 strings:
 *
 * <table>
 *   <caption>Record headers</caption>
 *   <tr><th>Header</th><th>Value</th></tr>
 *   <tr><td>{@value #HEADER_CONTENT_TYPE}</td><td>{@value Envelope#CONTENT_TYPE}</td></tr>
 *   <tr><td>{@value #HEADER_KIND}</td><td>{@code user} | {@code admin} ({@code kckind})</td></tr>
 *   <tr><td>{@value #HEADER_EVENT_TYPE}</td><td>CloudEvents {@code type}, e.g.
 *       {@code org.keycloak.event.LOGIN}, {@code org.keycloak.admin.USER.CREATE}</td></tr>
 *   <tr><td>{@value #HEADER_REALM_ID}</td><td>{@code kcrealmid}; omitted when absent</td></tr>
 *   <tr><td>{@value #HEADER_SCHEMA_VERSION}</td><td>{@code kcschemaversion} in decimal, e.g. {@code 1}</td></tr>
 * </table>
 *
 * <p>Pure (no {@code KeycloakSession}); immutable and thread-safe. Each record gets its own header byte
 * arrays.
 */
public final class EventRecordFactory {

    public static final String HEADER_CONTENT_TYPE = "content-type";
    public static final String HEADER_KIND = "kc-kind";
    public static final String HEADER_EVENT_TYPE = "kc-event-type";
    public static final String HEADER_REALM_ID = "kc-realm-id";
    public static final String HEADER_SCHEMA_VERSION = "kc-schema-version";

    private final TopicRouter topicRouter;
    private final KeyStrategy keyStrategy;

    public EventRecordFactory(TopicRouter topicRouter, KeyStrategy keyStrategy) {
        this.topicRouter = Objects.requireNonNull(topicRouter, "topicRouter");
        this.keyStrategy = Objects.requireNonNull(keyStrategy, "keyStrategy");
    }

    /**
     * Factory for the configured topics and the default key strategy.
     *
     * @throws hr.delmisoft.keycloak.kafka.config.KafkaEventsConfigException when a topic template is invalid
     */
    public static EventRecordFactory from(KafkaEventsConfig config) {
        return new EventRecordFactory(TopicRouter.from(config), new KeyStrategy());
    }

    /**
     * Builds the record.
     *
     * @param envelope the envelope (routing input)
     * @param value    its serialized form, {@code EnvelopeMapper.toJsonBytes(envelope)}; used as is, not copied
     */
    public ProducerRecord<String, byte[]> create(Envelope envelope, byte[] value) {
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(value, "value");
        RecordHeaders headers = new RecordHeaders();
        add(headers, HEADER_CONTENT_TYPE, Envelope.CONTENT_TYPE);
        add(headers, HEADER_KIND, envelope.kckind() == null ? null : envelope.kckind().value());
        add(headers, HEADER_EVENT_TYPE, envelope.type());
        add(headers, HEADER_REALM_ID, envelope.kcrealmid());
        add(headers, HEADER_SCHEMA_VERSION, Integer.toString(envelope.kcschemaversion()));
        return new ProducerRecord<>(
                topicRouter.topicFor(envelope), null, keyStrategy.keyFor(envelope), value, headers);
    }

    /**
     * The {@value #HEADER_KIND} header of a record built here ({@code user} or {@code admin}); {@code null} when
     * absent. Used to tag metrics.
     */
    public static String kindOf(ProducerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(HEADER_KIND);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void add(RecordHeaders headers, String name, String value) {
        if (value != null) {
            headers.add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
