package hr.delmisoft.keycloak.kafka.config;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** How events reach Kafka ({@code delivery-mode} option). */
public enum DeliveryMode {

    /**
     * At-most-once: after the Keycloak transaction commits, events go to a bounded in-memory queue drained
     * by a sender thread.
     */
    ASYNC;

    /** The option value, e.g. {@code async}. */
    public String optionValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** All option values. */
    public static List<String> names() {
        return Arrays.stream(values()).map(DeliveryMode::optionValue).toList();
    }
}
