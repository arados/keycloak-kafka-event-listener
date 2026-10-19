package hr.delmisoft.keycloak.kafka.envelope;

import java.util.Objects;

/**
 * Server-wide envelope settings, derived from the listener configuration.
 *
 * @param includeIpAddress      emit {@code ipAddress} (user events and admin {@code authDetails})
 * @param includeRepresentation emit admin {@code representation}; a second gate on top of Keycloak's own
 *                              per-event {@code includeRepresentation} flag, which is a hard ceiling
 * @param detailRedaction       which {@code details} values are replaced by {@link EnvelopeMapper#REDACTED}
 *                              (config {@code allowed-detail-keys} / {@code redacted-detail-keys})
 */
public record EnvelopeOptions(boolean includeIpAddress, boolean includeRepresentation, DetailRedaction detailRedaction) {

    /** Defaults matching the documented configuration defaults. */
    public static final EnvelopeOptions DEFAULTS = new EnvelopeOptions(true, false, DetailRedaction.DEFAULTS);

    public EnvelopeOptions {
        Objects.requireNonNull(detailRedaction, "detailRedaction");
    }
}
