package hr.delmisoft.keycloak.kafka.envelope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Details;

class DetailRedactionTest {

    private final DetailRedaction defaults = DetailRedaction.DEFAULTS;

    // ---- standard keys pass through ----

    @ParameterizedTest
    @ValueSource(strings = {
            // Details constants the old substring regex wrongly redacted (RESEARCH §3.3).
            "token_id", "refresh_token_id", "token_type", "refresh_token_type", "credential_type", "credential_id",
            "selected_credential_id", "credential_user_label", "access_token_expiration_time",
            "age_of_refresh_token", "requested_token_type", "subject_token_client_id", "verifiable_credential_format",
            // ordinary Details constants
            "username", "auth_method", "redirect_uri", "code_id", "grant_type", "scope", "reason",
            "client_auth_method", "previous_email", "updated_email"})
    void detailsConstantsPassThrough(String key) {
        assertThat(defaults.isRedacted(key), is(false));
    }

    @Test
    void everyDetailsConstantIsStandardAndNotRedactedByDefault() throws IllegalAccessException {
        List<String> redacted = new ArrayList<>();
        int count = 0;
        for (Field field : Details.class.getFields()) {
            if (field.getType() == String.class && Modifier.isStatic(field.getModifiers())) {
                String key = (String) field.get(null);
                count++;
                assertThat(DetailRedaction.STANDARD_KEYS, hasItem(key.toLowerCase(Locale.ROOT)));
                if (defaults.isRedacted(key)) {
                    redacted.add(key);
                }
            }
        }
        assertThat("Details has constants (reflection works)", count > 50, is(true));
        assertThat("standard keys redacted by default", redacted, is(empty()));
    }

    @Test
    void extraStandardKeysPassThrough() {
        for (String key : DetailRedaction.EXTRA_STANDARD_KEYS) {
            assertThat(key, defaults.isRedacted(key), is(false));
        }
        // As Keycloak spells them (constants are matched case-insensitively).
        assertThat(defaults.isRedacted("SAML_LOGOUT_REQUEST_ID"), is(false));
        assertThat(defaults.isRedacted("membershipType"), is(false));
        assertThat(defaults.isRedacted("apiVersion"), is(false));
    }

    @Test
    void extraStandardKeysAreLowercase() {
        assertThat(DetailRedaction.EXTRA_STANDARD_KEYS,
                everyItem(matchesPattern("[a-z0-9_-]+")));
        assertThat(DetailRedaction.BUILT_IN_REDACTED_KEYS,
                everyItem(matchesPattern("[a-z0-9_-]+")));
    }

    @Test
    void builtInDenylistDoesNotShadowStandardKeys() {
        // Not required for safety (the denylist wins), but a collision after a Keycloak bump means a standard
        // key would silently disappear for consumers: revisit the denylist then.
        Set<String> overlap = new HashSet<>(DetailRedaction.BUILT_IN_REDACTED_KEYS);
        overlap.retainAll(DetailRedaction.STANDARD_KEYS);

        assertThat(overlap, is(empty()));
    }

    // ---- built-in denylist ----

    @ParameterizedTest
    @ValueSource(strings = {
            "password", "PASSWORD", "password_new", "Password-Confirm", "new_password", "client_secret", "Secret",
            "code_verifier", "otp", "TOTP", "access_token", "refresh_token", "id_token", "subject_token",
            "client_assertion", "device_code"})
    void denylistedKeysAreRedacted(String key) {
        assertThat(defaults.isRedacted(key), is(true));
    }

    @Test
    void denylistWinsOverAllowlist() {
        DetailRedaction policy = new DetailRedaction(Set.of("password", "client_secret"), Set.of());

        assertThat(policy.isRedacted("password"), is(true));
        assertThat(policy.isRedacted("Client_Secret"), is(true));
    }

    // ---- unknown keys ----

    @ParameterizedTest
    @ValueSource(strings = {"custom_note", "totp_code", "my_secret_sauce", "x-api-key", "previous_phone", ""})
    void unknownKeysAreRedactedByDefault(String key) {
        assertThat(defaults.isRedacted(key), is(true));
    }

    @Test
    void allowlistedUnknownKeysPassThroughCaseInsensitively() {
        DetailRedaction policy = new DetailRedaction(Set.of("custom_note", "PREVIOUS_PHONE"), Set.of());

        assertThat(policy.isRedacted("custom_note"), is(false));
        assertThat(policy.isRedacted("Custom_Note"), is(false));
        assertThat(policy.isRedacted("previous_phone"), is(false));
        assertThat(policy.isRedacted("custom_other"), is(true));
    }

    // ---- configured redacted keys ----

    @Test
    void configuredRedactedKeysWinOverStandardKeys() {
        DetailRedaction policy = new DetailRedaction(Set.of(), Set.of("username", "EMAIL"));

        assertThat(policy.isRedacted("username"), is(true));
        assertThat(policy.isRedacted("Email"), is(true));
        assertThat(policy.isRedacted("auth_method"), is(false));
    }

    @Test
    void configuredRedactedKeysWinOverAllowedKeys() {
        DetailRedaction policy = new DetailRedaction(Set.of("custom_note"), Set.of("custom_note"));

        assertThat(policy.isRedacted("custom_note"), is(true));
    }

    // ---- construction ----

    @Test
    void configuredKeysAreLowercasedAndCopied() {
        Set<String> allowed = new HashSet<>(Set.of("Custom_Note"));
        DetailRedaction policy = new DetailRedaction(allowed, Set.of("UserName"));

        allowed.add("later_added");

        assertThat(policy.allowedKeys(), is(Set.of("custom_note")));
        assertThat(policy.redactedKeys(), is(Set.of("username")));
        assertThat(policy.isRedacted("later_added"), is(true));
        assertThrows(UnsupportedOperationException.class, () -> policy.allowedKeys().add("x"));
    }

    @Test
    void nullSetsAndKeysAreRejected() {
        assertThrows(NullPointerException.class, () -> new DetailRedaction(null, Set.of()));
        assertThrows(NullPointerException.class, () -> new DetailRedaction(Set.of(), null));
        Set<String> withNull = new HashSet<>(Arrays.asList("ok", null));
        assertThrows(NullPointerException.class, () -> new DetailRedaction(withNull, Set.of()));
    }

    @Test
    void defaultsHaveNoConfiguredKeys() {
        assertThat(DetailRedaction.DEFAULTS.allowedKeys(), is(empty()));
        assertThat(DetailRedaction.DEFAULTS.redactedKeys(), is(empty()));
        assertThat(EnvelopeOptions.DEFAULTS.detailRedaction(), is(DetailRedaction.DEFAULTS));
        assertThat(DetailRedaction.STANDARD_KEYS.stream().filter(k -> !k.equals(k.toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList()), is(empty()));
    }
}
