package hr.delmisoft.keycloak.kafka.routing;

import hr.delmisoft.keycloak.kafka.envelope.Envelope;

/**
 * Computes the Kafka record key, which decides the partition and therefore the ordering scope.
 *
 * <ul>
 *   <li>User event: {@code <realmId>:<userId>}; without a user {@code <realmId>:<sessionId>}; with neither
 *       {@code <realmId>}. All events of one user (or one anonymous session) stay in order.</li>
 *   <li>Admin event: {@code <realmId>:<first two resourcePath segments>}, e.g.
 *       {@code <realmId>:users/<id>} for {@code users/<id>/role-mappings/realm}, so all changes to one entity
 *       stay in order; a one-segment path is used as is; no path gives {@code <realmId>}.</li>
 * </ul>
 * Empty strings count as missing. A missing realm id (never seen from Keycloak) is treated as empty. The key
 * is never {@code null}, so Kafka never falls back to unkeyed (sticky) partitioning.
 *
 * <p>Stateless and thread-safe.
 */
public final class KeyStrategy {

    /** Separates the realm id from the rest of the key. */
    static final char SEPARATOR = ':';

    /** The record key for the envelope; never {@code null}. */
    public String keyFor(Envelope envelope) {
        String realmId = envelope.kcrealmid() == null ? "" : envelope.kcrealmid();
        String scope = switch (envelope.data()) {
            case Envelope.UserData user -> present(user.userId()) ? user.userId() : user.sessionId();
            case Envelope.AdminData admin -> entityPath(admin.resourcePath());
        };
        return present(scope) ? realmId + SEPARATOR + scope : realmId;
    }

    /** The first two segments of an admin resource path ({@code users/<id>/...} → {@code users/<id>}). */
    private static String entityPath(String resourcePath) {
        if (resourcePath == null) {
            return null;
        }
        int first = resourcePath.indexOf('/');
        if (first < 0) {
            return resourcePath;
        }
        int second = resourcePath.indexOf('/', first + 1);
        return second < 0 ? resourcePath : resourcePath.substring(0, second);
    }

    private static boolean present(String value) {
        return value != null && !value.isEmpty();
    }
}
