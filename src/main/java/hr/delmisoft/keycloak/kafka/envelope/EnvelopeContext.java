package hr.delmisoft.keycloak.kafka.envelope;

import java.util.Objects;

/**
 * Per-event values the mapper cannot derive from the Keycloak event itself. The provider computes them on
 * the request thread (where the {@code KeycloakSession} is available) so that {@link EnvelopeMapper} stays a
 * pure function.
 *
 * @param source    CloudEvents {@code source}, precomputed by the caller (derivation is decision D2 in
 *                  docs/PLAN.md); required
 * @param realmName realm name at event time ({@code kcrealmname}); {@code null} falls back to the name Keycloak
 *                  recorded on the event, and is omitted if that is {@code null} too
 */
public record EnvelopeContext(String source, String realmName) {

    public EnvelopeContext {
        Objects.requireNonNull(source, "source");
    }
}
