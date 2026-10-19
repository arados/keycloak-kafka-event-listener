package hr.delmisoft.keycloak.kafka.envelope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.util.JsonSerialization;

class EnvelopeMapperTest {

    private static final String REALM_ID = "6b1e8f4a-3c2d-4e5f-8a9b-0c1d2e3f4a5b";
    private static final String USER_ID = "f3e2d1c0-b9a8-4776-8554-433221100fed";
    private static final String SESSION_ID = "9d8c7b6a-5f4e-4d3c-8b2a-1f0e9d8c7b6a";
    private static final String CLIENT_ID = "kafka-demo-client";
    private static final String IP = "172.18.0.1";
    private static final EnvelopeContext CONTEXT = new EnvelopeContext("keycloak:" + REALM_ID, "kafka-demo");

    private static final String USER_REPRESENTATION = """
            {"username":"alice","enabled":true,"email":"alice@example.com","emailVerified":false,\
            "firstName":"Alice","lastName":"Liddell","attributes":{"department":["engineering"]}}""";

    private final EnvelopeMapper defaults = new EnvelopeMapper(EnvelopeOptions.DEFAULTS);

    // ---- golden fixtures: the public contract ----

    @Test
    void userLoginMatchesGoldenFixture() throws IOException {
        Event event = userEvent("0b6c1e2a-5d4f-4c3b-9a8e-7f6d5c4b3a21", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details(
                "auth_method", "openid-connect",
                "auth_type", "code",
                "redirect_uri", "http://localhost:3000/callback",
                "consent", "no_consent_required",
                "code_id", SESSION_ID,
                "username", "testuser"));

        assertMatchesFixture(defaults.map(event, CONTEXT), "user-login.json");
    }

    @Test
    void userLoginErrorMatchesGoldenFixture() throws IOException {
        Event event = userEvent("1c7d2f3b-6e5a-4d4c-8b9f-8a7e6d5c4b32", EventType.LOGIN_ERROR, "2026-10-19T12:35:00Z");
        event.setUserId(null);
        event.setSessionId(null);
        event.setError("user_not_found");
        event.setDetails(details(
                "auth_method", "openid-connect",
                "grant_type", "password",
                "username", "nobody"));

        assertMatchesFixture(defaults.map(event, CONTEXT), "user-login-error.json");
    }

    @Test
    void userEventWithRedactedDetailsAndNoIpMatchesGoldenFixture() throws IOException {
        Event event = userEvent("2d8e3a4c-7f6b-4e5d-9c0a-9b8f7e6d5c43", EventType.CODE_TO_TOKEN,
                "2026-10-19T12:34:57.001Z");
        event.setDetails(details(
                "token_id", "6a5b4c3d-2e1f-4a0b-9c8d-7e6f5a4b3c2d",
                "grant_type", "authorization_code",
                "refresh_token_type", "Refresh",
                "scope", "openid profile email",
                "refresh_token_id", "7b6c5d4e-3f2a-4b1c-8d9e-0f1a2b3c4d5e",
                "code_id", SESSION_ID,
                "client_auth_method", "client-secret",
                "code_verifier", "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
                "x_custom_extension_note", "set by a custom authenticator"));
        EnvelopeMapper noIp = new EnvelopeMapper(
                new EnvelopeOptions(false, false, DetailRedaction.DEFAULTS));

        assertMatchesFixture(noIp.map(event, CONTEXT), "user-redacted-no-ip.json");
    }

    @Test
    void adminCreateUserWithoutRepresentationMatchesGoldenFixture() throws IOException {
        // Keycloak offers the representation, but include-representation defaults to false.
        Envelope envelope = defaults.map(adminCreateUser(), true, CONTEXT);

        assertMatchesFixture(envelope, "admin-create-user.json");
    }

    @Test
    void adminCreateUserWithRepresentationMatchesGoldenFixture() throws IOException {
        EnvelopeMapper withRepresentation = new EnvelopeMapper(
                new EnvelopeOptions(true, true, DetailRedaction.DEFAULTS));

        Envelope envelope = withRepresentation.map(adminCreateUser(), true, CONTEXT);

        assertMatchesFixture(envelope, "admin-create-user-representation.json");
    }

    // ---- serialization details ----

    @Test
    void attributesAreSerializedInDocumentedOrder() throws IOException {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setError("some_error");
        event.setDetails(details("username", "testuser"));

        JsonNode json = toTree(defaults.map(event, CONTEXT));

        assertThat(fieldNames(json), contains(
                "specversion", "id", "source", "type", "time", "subject", "datacontenttype",
                "kcschemaversion", "kcrealmid", "kcrealmname", "kckind", "data"));
        assertThat(fieldNames(json.get("data")), contains(
                "type", "clientId", "userId", "sessionId", "ipAddress", "error", "details"));
    }

    @Test
    void adminDataFieldsAreSerializedInDocumentedOrder() throws IOException {
        AdminEvent event = adminCreateUser();
        event.setError("some_error");
        event.setDetails(details("k", "v"));
        EnvelopeMapper withRepresentation = new EnvelopeMapper(
                new EnvelopeOptions(true, true, DetailRedaction.DEFAULTS));

        JsonNode data = toTree(withRepresentation.map(event, true, CONTEXT)).get("data");

        assertThat(fieldNames(data), contains(
                "operationType", "resourceType", "resourcePath", "authDetails", "error", "details",
                "representation"));
        assertThat(fieldNames(data.get("authDetails")), contains(
                "realmId", "realmName", "clientId", "userId", "ipAddress"));
    }

    @Test
    void detailsAreSortedByKey() throws IOException {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details("zeta", "1", "alpha", "2", "mu", "3"));

        JsonNode details = toTree(defaults.map(event, CONTEXT)).get("data").get("details");

        assertThat(fieldNames(details), contains("alpha", "mu", "zeta"));
    }

    @Test
    void nullsAreOmittedNotWrittenAsNull() throws IOException {
        Event event = new Event();
        event.setId("id-1");
        event.setType(EventType.LOGOUT);
        event.setRealmId(REALM_ID);

        String json = new String(EnvelopeMapper.toJsonBytes(
                defaults.map(event, new EnvelopeContext("keycloak:" + REALM_ID, null))), StandardCharsets.UTF_8);

        assertThat(json.contains("null"), is(false));
        JsonNode tree = JsonSerialization.readValue(json, JsonNode.class);
        assertThat(tree.has("subject"), is(false));
        assertThat(tree.has("kcrealmname"), is(false));
        assertThat(fieldNames(tree.get("data")), contains("type"));
    }

    @Test
    void emptyDetailsAreOmitted() throws IOException {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(new HashMap<>());

        assertThat(toTree(defaults.map(event, CONTEXT)).get("data").has("details"), is(false));
    }

    @Test
    void nullDetailValuesAreDropped() throws IOException {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        Map<String, String> details = new HashMap<>();
        details.put("username", "testuser");
        details.put("redirect_uri", null);
        event.setDetails(details);

        JsonNode json = toTree(defaults.map(event, CONTEXT)).get("data").get("details");

        assertThat(fieldNames(json), contains("username"));
    }

    @Test
    void timeAlwaysHasMillisecondsInUtc() {
        assertThat(EnvelopeMapper.formatTime(0L), equalTo("1970-01-01T00:00:00.000Z"));
        assertThat(EnvelopeMapper.formatTime(Instant.parse("2026-10-19T12:34:56.7Z").toEpochMilli()),
                equalTo("2026-10-19T12:34:56.700Z"));
    }

    @Test
    void serializedBytesAreUtf8() throws IOException {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details("username", "željko"));

        JsonNode json = JsonSerialization.readValue(
                new String(EnvelopeMapper.toJsonBytes(defaults.map(event, CONTEXT)), StandardCharsets.UTF_8),
                JsonNode.class);

        assertThat(json.get("data").get("details").get("username").asText(), equalTo("željko"));
    }

    // ---- context ----

    @Test
    void realmNameFallsBackToEventWhenContextHasNone() {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setRealmName("from-event");

        assertThat(defaults.map(event, new EnvelopeContext("s", null)).kcrealmname(), equalTo("from-event"));
        assertThat(defaults.map(event, new EnvelopeContext("s", "from-context")).kcrealmname(),
                equalTo("from-context"));
    }

    @Test
    void sourceIsTakenFromContextVerbatim() {
        Envelope envelope = defaults.map(adminCreateUser(), false, new EnvelopeContext("https://kc/realms/x", "x"));

        assertThat(envelope.source(), equalTo("https://kc/realms/x"));
    }

    @Test
    void contextRequiresSource() {
        assertThrows(NullPointerException.class, () -> new EnvelopeContext(null, "kafka-demo"));
    }

    @Test
    void optionsRequireDetailRedaction() {
        assertThrows(NullPointerException.class, () -> new EnvelopeOptions(true, false, null));
    }

    // ---- redaction (policy details in DetailRedactionTest) ----

    @ParameterizedTest
    @ValueSource(strings = {
            "password", "PASSWORD_confirm", "client_secret", "Secret", "access_token", "otp", "totp",
            "code_verifier", "CODE_VERIFIER"})
    void defaultPolicyRedactsDenylistedKeys(String key) {
        assertThat(userDetailsAfterMapping(defaults, key), equalTo(EnvelopeMapper.REDACTED));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "username", "auth_method", "redirect_uri", "code_id", "grant_type", "scope", "token_id",
            "refresh_token_id", "token_type", "credential_type", "credential_id", "access_token_expiration_time"})
    void defaultPolicyKeepsStandardKeycloakKeys(String key) {
        assertThat(userDetailsAfterMapping(defaults, key), equalTo("value"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"custom_note", "totp_code", "x-api-key", "previous_phone"})
    void defaultPolicyRedactsUnknownKeys(String key) {
        assertThat(userDetailsAfterMapping(defaults, key), equalTo(EnvelopeMapper.REDACTED));
    }

    @Test
    void configuredAllowedAndRedactedKeysAreApplied() {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details(
                "username", "testuser", "token_id", "abc", "custom_note", "hello", "Custom_Ref", "r-1"));
        EnvelopeMapper custom = new EnvelopeMapper(new EnvelopeOptions(true, false,
                new DetailRedaction(Set.of("custom_note", "custom_ref"), Set.of("username"))));

        Envelope.UserData data = (Envelope.UserData) custom.map(event, CONTEXT).data();

        assertThat(data.details().get("username"), equalTo(EnvelopeMapper.REDACTED));
        assertThat(data.details().get("token_id"), equalTo("abc"));
        assertThat(data.details().get("custom_note"), equalTo("hello"));
        assertThat(data.details().get("Custom_Ref"), equalTo("r-1"));
    }

    @Test
    void adminDetailsAreRedactedToo() {
        AdminEvent event = adminCreateUser();
        event.setDetails(details("new_password", "hunter2", "reason", "reset", "custom_note", "hello"));

        Envelope.AdminData data = (Envelope.AdminData) defaults.map(event, false, CONTEXT).data();

        assertThat(data.details().get("new_password"), equalTo(EnvelopeMapper.REDACTED));
        assertThat(data.details().get("custom_note"), equalTo(EnvelopeMapper.REDACTED));
        assertThat(data.details().get("reason"), equalTo("reset"));
    }

    @Test
    void redactionDoesNotMutateTheKeycloakEvent() {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details("password", "hunter2"));

        defaults.map(event, CONTEXT);

        assertThat(event.getDetails().get("password"), equalTo("hunter2"));
    }

    @Test
    void secretValueNeverAppearsInSerializedBytes() {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details("client_secret", "s3cr3t-v4lu3"));

        String json = new String(EnvelopeMapper.toJsonBytes(defaults.map(event, CONTEXT)),
                StandardCharsets.UTF_8);

        assertThat(json.contains("s3cr3t-v4lu3"), is(false));
    }

    // ---- ip toggle ----

    @Test
    void ipAddressIsOmittedFromAdminAuthDetailsWhenDisabled() throws IOException {
        EnvelopeMapper noIp = new EnvelopeMapper(
                new EnvelopeOptions(false, false, DetailRedaction.DEFAULTS));

        JsonNode authDetails = toTree(noIp.map(adminCreateUser(), false, CONTEXT)).get("data").get("authDetails");

        assertThat(fieldNames(authDetails), not(hasItem("ipAddress")));
        assertThat(authDetails.get("userId").asText(), equalTo("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"));
    }

    @Test
    void missingAuthDetailsAreOmitted() throws IOException {
        AdminEvent event = adminCreateUser();
        event.setAuthDetails(null);

        assertThat(toTree(defaults.map(event, false, CONTEXT)).get("data").has("authDetails"), is(false));
    }

    // ---- representation ceiling ----

    @Test
    void representationIsOmittedWhenKeycloakDidNotIncludeIt() {
        EnvelopeMapper withRepresentation = new EnvelopeMapper(
                new EnvelopeOptions(true, true, DetailRedaction.DEFAULTS));

        Envelope.AdminData data = (Envelope.AdminData) withRepresentation.map(adminCreateUser(), false, CONTEXT).data();

        assertThat(data.representation(), nullValue());
    }

    @Test
    void representationIsOmittedWhenConfigDisablesIt() {
        Envelope.AdminData data = (Envelope.AdminData) defaults.map(adminCreateUser(), true, CONTEXT).data();

        assertThat(data.representation(), nullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"not json", "{\"unterminated\": ", "", "   ", "null"})
    void unparsableOrEmptyRepresentationIsOmitted(String representation) throws IOException {
        AdminEvent event = adminCreateUser();
        event.setRepresentation(representation);
        EnvelopeMapper withRepresentation = new EnvelopeMapper(
                new EnvelopeOptions(true, true, DetailRedaction.DEFAULTS));

        JsonNode data = toTree(withRepresentation.map(event, true, CONTEXT)).get("data");

        assertThat(data.has("representation"), is(false));
        assertThat(data.get("resourcePath").asText(), equalTo("users/a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"));
    }

    @Test
    void arrayRepresentationIsEmbedded() throws IOException {
        AdminEvent event = adminCreateUser();
        event.setRepresentation("[{\"name\":\"offline_access\"}]");
        EnvelopeMapper withRepresentation = new EnvelopeMapper(
                new EnvelopeOptions(true, true, DetailRedaction.DEFAULTS));

        JsonNode representation = toTree(withRepresentation.map(event, true, CONTEXT)).get("data").get("representation");

        assertThat(representation.isArray(), is(true));
        assertThat(representation.get(0).get("name").asText(), equalTo("offline_access"));
    }

    // ---- types and subjects ----

    @Test
    void adminTypeUsesResourceTypeAndOperation() {
        AdminEvent event = adminCreateUser();
        event.setResourceType(ResourceType.REALM_ROLE_MAPPING);
        event.setOperationType(OperationType.DELETE);

        Envelope envelope = defaults.map(event, false, CONTEXT);

        assertThat(envelope.type(), equalTo("org.keycloak.admin.REALM_ROLE_MAPPING.DELETE"));
        assertThat(envelope.kckind(), equalTo(Envelope.Kind.ADMIN));
        assertThat(envelope.subject(), equalTo(event.getResourcePath()));
    }

    @Test
    void customAdminResourceTypeIsKeptVerbatim() {
        AdminEvent event = adminCreateUser();
        event.setResourceTypeAsString("ORGANIZATION_MEMBERSHIP_X");

        assertThat(defaults.map(event, false, CONTEXT).type(),
                equalTo("org.keycloak.admin.ORGANIZATION_MEMBERSHIP_X.CREATE"));
    }

    @Test
    void userEnvelopeCarriesKindAndSchemaVersion() throws IOException {
        JsonNode json = toTree(defaults.map(userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z"), CONTEXT));

        assertThat(json.get("kckind").asText(), equalTo("user"));
        assertThat(json.get("kcschemaversion").isInt(), is(true));
        assertThat(json.get("kcschemaversion").asInt(), equalTo(Envelope.SCHEMA_VERSION));
        assertThat(json.get("specversion").asText(), equalTo("1.0"));
        assertThat(json.get("data").has("error"), is(false));
    }

    // ---- helpers ----

    /** Maps a LOGIN event whose only detail is {@code key=value} and returns that detail's emitted value. */
    private static String userDetailsAfterMapping(EnvelopeMapper mapper, String key) {
        Event event = userEvent("id-1", EventType.LOGIN, "2026-10-19T12:34:56.789Z");
        event.setDetails(details(key, "value"));
        return ((Envelope.UserData) mapper.map(event, CONTEXT).data()).details().get(key);
    }

    private static Event userEvent(String id, EventType type, String time) {
        Event event = new Event();
        event.setId(id);
        event.setType(type);
        event.setTime(Instant.parse(time).toEpochMilli());
        event.setRealmId(REALM_ID);
        event.setRealmName("kafka-demo");
        event.setClientId(CLIENT_ID);
        event.setUserId(USER_ID);
        event.setSessionId(SESSION_ID);
        event.setIpAddress(IP);
        return event;
    }

    private static AdminEvent adminCreateUser() {
        AuthDetails auth = new AuthDetails();
        auth.setRealmId("11111111-2222-4333-8444-555555555555");
        auth.setRealmName("master");
        auth.setClientId("0f9e8d7c-6b5a-4948-b736-251403f2e1d0");
        auth.setUserId("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
        auth.setIpAddress(IP);

        AdminEvent event = new AdminEvent();
        event.setId("3e9f4b5d-8a7c-4f6e-ad1b-ac9f8e7d6c54");
        event.setTime(Instant.parse("2026-10-19T12:40:01.234Z").toEpochMilli());
        event.setRealmId(REALM_ID);
        event.setRealmName("kafka-demo");
        event.setAuthDetails(auth);
        event.setOperationType(OperationType.CREATE);
        event.setResourceType(ResourceType.USER);
        event.setResourcePath("users/a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d");
        event.setRepresentation(USER_REPRESENTATION);
        return event;
    }

    /** HashMap on purpose: Keycloak's details map has no defined order; the mapper must impose one. */
    private static Map<String, String> details(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static JsonNode toTree(Envelope envelope) throws IOException {
        return JsonSerialization.readValue(EnvelopeMapper.toJsonBytes(envelope), JsonNode.class);
    }

    private static List<String> fieldNames(JsonNode node) {
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static void assertMatchesFixture(Envelope envelope, String fixture) throws IOException {
        JsonNode expected;
        try (InputStream in = EnvelopeMapperTest.class.getResourceAsStream("/envelope/" + fixture)) {
            assertThat("fixture " + fixture, in, not(nullValue()));
            expected = JsonSerialization.readValue(in, JsonNode.class);
        }
        JsonNode actual = toTree(envelope);
        assertThat("envelope vs. golden fixture " + fixture + "\nactual: " + actual, actual, equalTo(expected));
    }
}
