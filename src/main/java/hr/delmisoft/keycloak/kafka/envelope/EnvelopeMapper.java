package hr.delmisoft.keycloak.kafka.envelope;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.util.JsonSerialization;

/**
 * Maps Keycloak {@link Event}s and {@link AdminEvent}s to {@link Envelope}s and serializes them.
 *
 * <p>Pure: it reads only its arguments and never touches {@code KeycloakSession} / {@code RealmModel}, so it
 * can be called anywhere and tested without Keycloak. It never mutates the input event. Thread-safe.
 *
 * <p>Secrets: {@code details} values are replaced by {@link #REDACTED} when
 * {@link DetailRedaction#isRedacted(String)} says so for their key (denylisted and unknown keys); the key is
 * kept so consumers can see the detail existed. Admin {@code representation} is emitted only when Keycloak
 * passed {@code includeRepresentation=true} <em>and</em> {@link EnvelopeOptions#includeRepresentation()} is
 * set; a representation that does not parse as JSON is omitted rather than emitted as a string.
 */
public final class EnvelopeMapper {

    /** Replacement value for redacted {@code details} entries. */
    public static final String REDACTED = "[redacted]";

    /** RFC 3339, UTC, always with milliseconds (Keycloak event time has millisecond precision). */
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);

    private final EnvelopeOptions options;

    public EnvelopeMapper(EnvelopeOptions options) {
        this.options = Objects.requireNonNull(options, "options");
    }

    /** Maps a user event. */
    public Envelope map(Event event, EnvelopeContext context) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(context, "context");
        String eventType = event.getType() == null ? null : event.getType().name();
        Envelope.UserData data = new Envelope.UserData(
                eventType,
                event.getClientId(),
                event.getUserId(),
                event.getSessionId(),
                options.includeIpAddress() ? event.getIpAddress() : null,
                event.getError(),
                redact(event.getDetails()));
        return new Envelope(
                Envelope.SPEC_VERSION,
                event.getId(),
                context.source(),
                Envelope.USER_TYPE_PREFIX + eventType,
                formatTime(event.getTime()),
                event.getUserId(),
                Envelope.DATA_CONTENT_TYPE,
                Envelope.SCHEMA_VERSION,
                event.getRealmId(),
                context.realmName() != null ? context.realmName() : event.getRealmName(),
                Envelope.Kind.USER,
                data);
    }

    /**
     * Maps an admin event.
     *
     * @param includeRepresentation the flag Keycloak passed to
     *                              {@code EventListenerProvider.onEvent(AdminEvent, boolean)}; {@code false}
     *                              suppresses the representation regardless of configuration
     */
    public Envelope map(AdminEvent event, boolean includeRepresentation, EnvelopeContext context) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(context, "context");
        String resourceType = event.getResourceTypeAsString();
        String operationType = event.getOperationType() == null ? null : event.getOperationType().name();
        JsonNode representation = includeRepresentation && options.includeRepresentation()
                ? parseRepresentation(event.getRepresentation())
                : null;
        Envelope.AdminData data = new Envelope.AdminData(
                operationType,
                resourceType,
                event.getResourcePath(),
                authDetails(event.getAuthDetails()),
                event.getError(),
                redact(event.getDetails()),
                representation);
        return new Envelope(
                Envelope.SPEC_VERSION,
                event.getId(),
                context.source(),
                Envelope.ADMIN_TYPE_PREFIX + resourceType + "." + operationType,
                formatTime(event.getTime()),
                event.getResourcePath(),
                Envelope.DATA_CONTENT_TYPE,
                Envelope.SCHEMA_VERSION,
                event.getRealmId(),
                context.realmName() != null ? context.realmName() : event.getRealmName(),
                Envelope.Kind.ADMIN,
                data);
    }

    /** Serializes an envelope to UTF-8 JSON (the Kafka record value) with Keycloak's Jackson. */
    public static byte[] toJsonBytes(Envelope envelope) {
        try {
            return JsonSerialization.writeValueAsBytes(envelope);
        } catch (IOException e) {
            // Our records contain only strings, ints, string maps and parsed JSON trees: not reachable.
            throw new UncheckedIOException("Cannot serialize envelope " + envelope.id(), e);
        }
    }

    static String formatTime(long epochMillis) {
        return TIME_FORMAT.format(Instant.ofEpochMilli(epochMillis));
    }

    private Envelope.AuthDetails authDetails(AuthDetails auth) {
        if (auth == null) {
            return null;
        }
        return new Envelope.AuthDetails(
                auth.getRealmId(),
                auth.getRealmName(),
                auth.getClientId(),
                auth.getUserId(),
                options.includeIpAddress() ? auth.getIpAddress() : null);
    }

    /** Copy with sensitive values replaced and null keys/values dropped; {@code null} when empty. */
    private Map<String, String> redact(Map<String, String> details) {
        if (details == null || details.isEmpty()) {
            return null;
        }
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> entry : details.entrySet()) {
            String key = entry.getKey();
            if (key == null || entry.getValue() == null) {
                continue;
            }
            out.put(key, options.detailRedaction().isRedacted(key) ? REDACTED : entry.getValue());
        }
        return out.isEmpty() ? null : out;
    }

    private static JsonNode parseRepresentation(String representation) {
        if (representation == null || representation.isBlank()) {
            return null;
        }
        try {
            JsonNode node = JsonSerialization.readValue(representation, JsonNode.class);
            return node == null || node.isNull() || node.isMissingNode() ? null : node;
        } catch (IOException e) {
            // Not JSON: omit rather than leak an opaque string. Never log the payload itself.
            return null;
        }
    }
}
