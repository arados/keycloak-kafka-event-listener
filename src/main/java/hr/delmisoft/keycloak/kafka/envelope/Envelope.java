package hr.delmisoft.keycloak.kafka.envelope;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Envelope v1: a CloudEvents 1.0 event in structured JSON mode. This is the public contract consumers
 * read from Kafka (docs/PLAN.md "Envelope v1"); removing or renaming a field requires a new
 * {@link #SCHEMA_VERSION} and a MAJOR release. Golden fixtures in {@code src/test/resources/envelope/}
 * guard the shape.
 *
 * <p>Null policy: attributes and data fields that are {@code null} are omitted from the JSON, never
 * written as {@code null}. Empty {@code details} maps are omitted too.
 *
 * <p>Instances are built by {@link EnvelopeMapper}; serialize them with {@link EnvelopeMapper#toJsonBytes}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "specversion", "id", "source", "type", "time", "subject", "datacontenttype",
        "kcschemaversion", "kcrealmid", "kcrealmname", "kckind", "data"})
public record Envelope(
        String specversion,
        String id,
        String source,
        String type,
        String time,
        String subject,
        String datacontenttype,
        int kcschemaversion,
        String kcrealmid,
        String kcrealmname,
        Kind kckind,
        Data data) {

    /** CloudEvents spec version this envelope implements. */
    public static final String SPEC_VERSION = "1.0";

    /** Version of our {@code data} / extension shape ({@code kcschemaversion}). */
    public static final int SCHEMA_VERSION = 1;

    /** {@code datacontenttype} of {@link #data()}. */
    public static final String DATA_CONTENT_TYPE = "application/json";

    /** Kafka {@code content-type} header for a structured-mode CloudEvent. */
    public static final String CONTENT_TYPE = "application/cloudevents+json; charset=UTF-8";

    /** CloudEvents {@code type} prefix for user events; followed by the {@code EventType} name. */
    public static final String USER_TYPE_PREFIX = "org.keycloak.event.";

    /** CloudEvents {@code type} prefix for admin events; followed by {@code <RESOURCE_TYPE>.<OPERATION>}. */
    public static final String ADMIN_TYPE_PREFIX = "org.keycloak.admin.";

    /** Which Keycloak event family the envelope carries ({@code kckind}). */
    public enum Kind {
        USER("user"),
        ADMIN("admin");

        private final String value;

        Kind(String value) {
            this.value = value;
        }

        /** Wire value, also used for the {@code kc-kind} header. */
        @JsonValue
        public String value() {
            return value;
        }
    }

    /** The {@code data} member: {@link UserData} or {@link AdminData}. */
    public sealed interface Data permits UserData, AdminData {
    }

    /**
     * {@code data} of a user event.
     *
     * @param details Keycloak event details after redaction (copied, sorted by key); {@code null} when there are none
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"type", "clientId", "userId", "sessionId", "ipAddress", "error", "details"})
    public record UserData(
            String type,
            String clientId,
            String userId,
            String sessionId,
            String ipAddress,
            String error,
            Map<String, String> details) implements Data {

        public UserData {
            // Defensive, key-sorted (deterministic JSON), unmodifiable copy.
            details = details == null ? null : Collections.unmodifiableSortedMap(new TreeMap<>(details));
        }
    }

    /**
     * {@code data} of an admin event.
     *
     * @param details        admin event details after redaction (copied, sorted by key); {@code null} when there are none
     * @param representation the resource representation Keycloak recorded (already stripped of secrets by
     *                       Keycloak), embedded as JSON; present only when allowed by both Keycloak and our
     *                       configuration and when it parses as JSON
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({
            "operationType", "resourceType", "resourcePath", "authDetails", "error", "details", "representation"})
    public record AdminData(
            String operationType,
            String resourceType,
            String resourcePath,
            AuthDetails authDetails,
            String error,
            Map<String, String> details,
            JsonNode representation) implements Data {

        public AdminData {
            // Defensive, key-sorted (deterministic JSON), unmodifiable copy.
            details = details == null ? null : Collections.unmodifiableSortedMap(new TreeMap<>(details));
        }
    }

    /** Who performed an admin operation. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"realmId", "realmName", "clientId", "userId", "ipAddress"})
    public record AuthDetails(
            String realmId,
            String realmName,
            String clientId,
            String userId,
            String ipAddress) {
    }

}
