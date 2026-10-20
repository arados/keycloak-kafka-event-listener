package hr.delmisoft.keycloak.kafka.envelope;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.keycloak.events.Details;

/**
 * Decides which event {@code details} values are replaced by {@link EnvelopeMapper#REDACTED}
 * (README "Privacy"). Keys are compared case-insensitively and exactly (no
 * substring or prefix matching). A key is redacted when, in order of precedence:
 *
 * <ol>
 *   <li>it is in {@link #BUILT_IN_REDACTED_KEYS} or in {@link #redactedKeys()} (config
 *       {@code redacted-detail-keys}) &rarr; <b>redacted</b>, even if it is a standard or allowed key;</li>
 *   <li>it is a {@linkplain #STANDARD_KEYS standard Keycloak key} or in {@link #allowedKeys()} (config
 *       {@code allowed-detail-keys}) &rarr; passed through;</li>
 *   <li>otherwise (unknown key, typically set by a custom extension) &rarr; <b>redacted</b>.</li>
 * </ol>
 *
 * <p>Unknown keys are redacted because we cannot know what an extension stores; operators who know a
 * custom key is safe allowlist it. Immutable and thread-safe.
 *
 * @param allowedKeys  extra keys to pass through (lowercased here; never weakens the redacted lists)
 * @param redactedKeys extra keys to always redact (lowercased here; wins over everything else)
 */
public record DetailRedaction(Set<String> allowedKeys, Set<String> redactedKeys) {

    /**
     * Keys always redacted, whatever the configuration says: names under which a credential, token or other
     * bearer secret would be stored. Keycloak core never sets any of them as a detail (they are request
     * parameter / form field / model field names); they are here so that an extension copying request data
     * into {@code details} cannot leak it, and so that an allowlist entry cannot undo it.
     */
    public static final Set<String> BUILT_IN_REDACTED_KEYS = Set.of(
            // Password form fields (login-update-password.ftl uses password-new/password-confirm) and
            // common spellings.
            "password", "new_password", "current_password", "password_new", "password_confirm",
            "password-new", "password-confirm",
            // Client secrets and stored credential material (CredentialModel secretData/credentialData).
            "secret", "client_secret", "secret_data", "credential_data",
            // One-time codes (OTP form field "otp", update-totp form field "totp").
            "otp", "totp",
            // PKCE verifier: with the authorization code it redeems tokens.
            "code_verifier",
            // OAuth / OIDC bearer tokens and token-exchange inputs (RFC 7009/7662 "token", RFC 8693).
            "token", "access_token", "refresh_token", "id_token", "subject_token", "actor_token",
            // Client authentication and grant assertions (RFC 7521/7523), device and pre-authorized codes.
            "client_assertion", "assertion", "device_code", "pre-authorized_code");

    /**
     * Detail keys Keycloak 26.8.1 core sets that are <em>not</em> constants of {@link Details}: string literals
     * or constants of other classes. Paths are relative to the Keycloak source tree at tag 26.8.1; re-check on
     * Keycloak bumps ({@code git grep -nE '(^|[^A-Za-z])detail\('}). Custom user-profile attributes produce
     * {@code previous_<attr>} / {@code updated_<attr>} keys (EventAuditingAttributeChangeListener) that are
     * realm-specific and therefore not listed: allowlist them if wanted.
     */
    public static final Set<String> EXTRA_STANDARD_KEYS = Set.of(
            // services/.../authentication/authenticators/broker/IdpDetectExistingBrokerUserAuthenticator.java:67
            "authenticator",
            // services/.../broker/oidc/AbstractOAuth2IdentityProvider.java:923, OIDCIdentityProvider.java:1027,
            // services/.../social/bitbucket/BitbucketIdentityProvider.java:77
            "validation_method",
            // services/.../protocol/oidc/endpoints/AuthorizationEndpointChecker.java:481,
            // TokenIntrospectionEndpoint.java:212, services/.../protocol/oidc/par/endpoints/AbstractParEndpoint.java:97
            "detail",
            // services/.../protocol/oidc/grants/device/endpoints/DeviceEndpoint.java:259
            "device_code_user_session_id",
            // services/.../authentication/authenticators/broker/IdpCreateUserIfUniqueAuthenticator.java:146
            // ("existing_" + UserModel.EMAIL / UserModel.USERNAME, lines 167-190)
            "existing_email", "existing_username",
            // Details.PREF_PREVIOUS / PREF_UPDATED + UserModel.USERNAME:
            // services/.../authenticators/broker/IdpReviewProfileAuthenticator.java:216-217,
            // services/.../userprofile/EventAuditingAttributeChangeListener.java:59
            "previous_username", "updated_username",
            // Details.PREF_PREVIOUS / PREF_UPDATED + UserModel.LOCALE: services/.../locale/DefaultLocaleUpdaterProvider.java:49-50
            "previous_locale", "updated_locale",
            // Details.PREF_PREVIOUS + Details.IDENTITY_PROVIDER_USERNAME:
            // services/.../services/resources/IdentityBrokerService.java:994
            "previous_identity_provider_identity",
            // WebAuthnConstants (services/src/main/java/org/keycloak/WebAuthnConstants.java:67-83), used by
            // authenticators/browser/WebAuthnAuthenticator.java and requiredactions/WebAuthnRegister.java
            "public_key_credential_id", "public_key_credential_label", "public_key_credential_aaguid",
            "web_authn_authenticator_user_verification_checked", "web_authn_authenticated_user_id",
            "first_authenticated_user_id", "web_authn_authentication_error", "web_authn_authentication_error_detail",
            "web_authn_registration_error", "web_authn_registration_error_detail",
            // MembershipType.NAME (core/.../representations/idm/MembershipType.java:31):
            // services/.../organization/admin/resource/OrganizationMemberResource.java:275
            "membershiptype",
            // OAuth2Constants.SUBJECT_ISSUER: services/.../protocol/oidc/tokenexchange/AbstractTokenExchangeProvider.java:143
            "subject_issuer",
            // SamlProtocol.SAML_LOGOUT_REQUEST_ID: services/.../protocol/saml/SamlProtocol.java:805
            "saml_logout_request_id",
            // AdminEventV2Builder.API_VERSION_DETAIL_KEY: rest/admin-v2/services/.../events/admin/v2/AdminEventV2Builder.java:43
            "apiversion");

    /**
     * Keys passed through by default (lowercased): the values of all {@code String} constants of
     * {@link Details}, read reflectively so the set tracks the Keycloak version we run on, plus
     * {@link #EXTRA_STANDARD_KEYS}. {@code Details} also holds a few values and prefixes ({@code "user"},
     * {@code "no_consent_required"}, {@code "previous_"}, …); they are harmless as keys and kept for simplicity.
     */
    public static final Set<String> STANDARD_KEYS = standardKeys();

    /** No extra allowed or redacted keys (the documented configuration defaults). */
    public static final DetailRedaction DEFAULTS = new DetailRedaction(Set.of(), Set.of());

    public DetailRedaction {
        allowedKeys = Set.copyOf(lowercase(Objects.requireNonNull(allowedKeys, "allowedKeys")));
        redactedKeys = Set.copyOf(lowercase(Objects.requireNonNull(redactedKeys, "redactedKeys")));
    }

    /** Whether the value of the {@code details} entry with this key must be replaced by {@link EnvelopeMapper#REDACTED}. */
    public boolean isRedacted(String key) {
        String normalized = key.toLowerCase(Locale.ROOT);
        if (BUILT_IN_REDACTED_KEYS.contains(normalized) || redactedKeys.contains(normalized)) {
            return true;
        }
        return !STANDARD_KEYS.contains(normalized) && !allowedKeys.contains(normalized);
    }

    private static Set<String> lowercase(Set<String> keys) {
        Set<String> out = new HashSet<>();
        for (String key : keys) {
            out.add(Objects.requireNonNull(key, "detail key").toLowerCase(Locale.ROOT));
        }
        return out;
    }

    private static Set<String> standardKeys() {
        Set<String> keys = new HashSet<>(EXTRA_STANDARD_KEYS);
        for (Field field : Details.class.getFields()) {
            int modifiers = field.getModifiers();
            if (field.getType() == String.class && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)) {
                try {
                    keys.add(((String) field.get(null)).toLowerCase(Locale.ROOT));
                } catch (IllegalAccessException e) {
                    // getFields() returns public members of a public interface only: not reachable.
                    throw new IllegalStateException("Cannot read " + field, e);
                }
            }
        }
        return Set.copyOf(keys);
    }
}
