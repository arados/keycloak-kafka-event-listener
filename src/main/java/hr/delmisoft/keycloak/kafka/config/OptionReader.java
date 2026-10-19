package hr.delmisoft.keycloak.kafka.config;

import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.OPTION_PREFIX;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.keycloak.Config;

/**
 * Typed, validating access to our own (non-passthrough) options. Everything is read as a string via
 * {@link Config.Scope#get(String)} and parsed here, so every error names the option the way an operator
 * types it and shows the rejected value.
 */
final class OptionReader {

    private final Config.Scope scope;

    OptionReader(Config.Scope scope) {
        this.scope = scope;
    }

    /** Option name as typed in keycloak.conf / on the CLI (without the leading {@code --}). */
    static String optionName(String key) {
        return OPTION_PREFIX + key;
    }

    static KafkaEventsConfigException invalid(String key, String value, String reason) {
        return KafkaEventsConfigException.invalidValue(key, value, reason);
    }

    /** Trimmed value, or {@code null} when unset. A set-but-blank value is returned as {@code ""}. */
    String raw(String key) {
        String value = scope.get(key);
        return value == null ? null : value.trim();
    }

    String string(String key, String defaultValue) {
        String value = raw(key);
        if (value == null) {
            return defaultValue;
        }
        if (value.isEmpty()) {
            throw invalid(key, value, "must not be blank");
        }
        return value;
    }

    boolean bool(String key, boolean defaultValue) {
        String value = raw(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw invalid(key, value, "must be true or false");
        };
    }

    int positiveInt(String key, int defaultValue) {
        String value = raw(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException e) {
            // fall through to the common error
        }
        throw invalid(key, value, "must be an integer greater than 0");
    }

    long nonNegativeLong(String key, long defaultValue) {
        String value = raw(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed >= 0) {
                return parsed;
            }
        } catch (NumberFormatException e) {
            // fall through to the common error
        }
        throw invalid(key, value, "must be an integer greater than or equal to 0");
    }

    /** Comma-separated list: entries trimmed, empty entries (e.g. a trailing comma) ignored. */
    List<String> list(String key) {
        String value = raw(key);
        List<String> result = new ArrayList<>();
        if (value == null) {
            return result;
        }
        for (String entry : value.split(",", -1)) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}
