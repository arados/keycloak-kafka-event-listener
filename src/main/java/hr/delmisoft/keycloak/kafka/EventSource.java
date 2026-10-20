package hr.delmisoft.keycloak.kafka;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.keycloak.common.enums.SslRequired;
import org.keycloak.common.util.UriUtils;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.services.Urls;
import org.keycloak.urls.UrlType;

/**
 * Computes the CloudEvents {@code source} of an event (decision D2 in docs/PLAN.md, research in
 * docs/RESEARCH.md §3.2): the issuer URL of the event's realm, i.e. the {@code iss} Keycloak puts into that
 * realm's tokens, {@code <frontend base>/realms/<realm name>}; {@code urn:keycloak:realm:<realmId>} when no URL
 * can be resolved.
 *
 * <p>Must be called inside {@code onEvent}: it reads the session's context and realm. It never throws.
 *
 * <ul>
 *   <li>Context realm = event realm (user events, error events in Keycloak's inner session, brute-force
 *       events, admin events on resources of a realm): the context's frontend base URL, to which Keycloak's
 *       hostname provider has already applied the realm's {@code frontendUrl} attribute. This is exactly how
 *       Keycloak computes the token {@code iss}.</li>
 *   <li>Context realm ≠ event realm (realm DELETE: the event carries the admin's realm while the context is the
 *       deleted realm; no context realm at all): the event realm's own {@code frontendUrl} if set and valid,
 *       else the context's frontend base — unless that base came from the <em>context</em> realm's
 *       {@code frontendUrl}, which says nothing about the event realm; then the URN fallback.</li>
 *   <li>No HTTP request and no fixed {@code hostname} URL ({@code ContextNotActiveException}), realm not found,
 *       anything else unexpected: the URN fallback.</li>
 * </ul>
 */
final class EventSource {

    /** Realm attribute holding the realm's frontend URL override (Realm settings → General → Frontend URL). */
    static final String FRONTEND_URL_ATTRIBUTE = "frontendUrl";

    static final String URN_PREFIX = "urn:keycloak:realm:";

    private EventSource() {
    }

    /**
     * Resolves the event realm and its {@code source}.
     *
     * @param realmId   the event's realm id
     * @param realmName the realm name Keycloak recorded on the event; {@code null} → read from the realm model
     */
    static Resolved resolve(KeycloakSession session, String realmId, String realmName) {
        if (realmId == null) {
            return new Resolved(fallback(null), realmName);
        }
        String name = realmName;
        try {
            KeycloakContext context = session.getContext();
            RealmModel contextRealm = context.getRealm();
            boolean contextIsEventRealm = contextRealm != null && realmId.equals(contextRealm.getId());
            RealmModel eventRealm = contextIsEventRealm ? contextRealm : session.realms().getRealm(realmId);
            if (name == null && eventRealm != null) {
                name = eventRealm.getName();
            }
            if (name == null) {
                return new Resolved(fallback(realmId), null);
            }
            URI base;
            if (contextIsEventRealm) {
                base = context.getUri(UrlType.FRONTEND).getBaseUri();
            } else {
                base = frontendUrl(eventRealm);
                if (base == null) {
                    if (frontendUrl(contextRealm) != null) {
                        return new Resolved(fallback(realmId), name);
                    }
                    base = context.getUri(UrlType.FRONTEND).getBaseUri();
                }
            }
            return new Resolved(Urls.realmIssuer(base, name), name);
        } catch (RuntimeException e) {
            // ContextNotActiveException (no HTTP request, e.g. a scheduled task), a realm deleted in this
            // transaction, a malformed base URI... The fallback is part of the documented contract.
            return new Resolved(fallback(realmId), name);
        }
    }

    /** {@code urn:keycloak:realm:<realmId>}, the id percent-encoded where needed (UUIDs never are). */
    static String fallback(String realmId) {
        if (realmId == null || realmId.isEmpty()) {
            return "urn:keycloak:realm";
        }
        StringBuilder out = new StringBuilder(URN_PREFIX.length() + realmId.length());
        out.append(URN_PREFIX);
        for (byte b : realmId.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '.' || c == '_' || c == '~') {
                out.append((char) c);
            } else {
                out.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
                        .append(Character.toUpperCase(Character.forDigit(c & 0xf, 16)));
            }
        }
        return out.toString();
    }

    /**
     * The realm's {@code frontendUrl} as Keycloak's hostname provider would use it: validated with the same
     * check, default ports removed; {@code null} when unset or invalid (Keycloak ignores an invalid one too).
     */
    private static URI frontendUrl(RealmModel realm) {
        if (realm == null) {
            return null;
        }
        String url = realm.getAttribute(FRONTEND_URL_ATTRIBUTE);
        if (url == null || url.isBlank()) {
            return null;
        }
        try {
            UriUtils.checkUrl(SslRequired.NONE, url, FRONTEND_URL_ATTRIBUTE);
            URI uri = URI.create(url);
            boolean defaultPort = ("http".equalsIgnoreCase(uri.getScheme()) && uri.getPort() == 80)
                    || ("https".equalsIgnoreCase(uri.getScheme()) && uri.getPort() == 443);
            return defaultPort
                    ? new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), -1, uri.getPath(), uri.getQuery(),
                            uri.getFragment())
                    : uri;
        } catch (IllegalArgumentException | URISyntaxException e) {
            return null;
        }
    }

    /**
     * @param source    CloudEvents {@code source}; never {@code null}
     * @param realmName the event realm's name ({@code kcrealmname}); {@code null} when unknown
     */
    record Resolved(String source, String realmName) {
    }
}
