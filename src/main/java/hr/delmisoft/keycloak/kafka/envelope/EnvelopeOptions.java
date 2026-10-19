package hr.delmisoft.keycloak.kafka.envelope;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Server-wide envelope settings, derived from the listener configuration.
 *
 * @param includeIpAddress      emit {@code ipAddress} (user events and admin {@code authDetails})
 * @param includeRepresentation emit admin {@code representation}; a second gate on top of Keycloak's own
 *                              per-event {@code includeRepresentation} flag, which is a hard ceiling
 * @param redactedDetailKeys    a {@code details} entry whose key contains a match ({@link java.util.regex.Matcher#find()})
 *                              has its value replaced by {@link EnvelopeMapper#REDACTED}
 */
public record EnvelopeOptions(boolean includeIpAddress, boolean includeRepresentation, Pattern redactedDetailKeys) {

    /** Default redaction rule for {@code details} keys (docs/PLAN.md "Envelope v1"). */
    public static final Pattern DEFAULT_REDACTED_DETAIL_KEYS =
            Pattern.compile("(?i)(password|secret|token|credential|otp|code_verifier)");

    /** Defaults matching the documented configuration defaults. */
    public static final EnvelopeOptions DEFAULTS = new EnvelopeOptions(true, false, DEFAULT_REDACTED_DETAIL_KEYS);

    public EnvelopeOptions {
        Objects.requireNonNull(redactedDetailKeys, "redactedDetailKeys");
    }
}
