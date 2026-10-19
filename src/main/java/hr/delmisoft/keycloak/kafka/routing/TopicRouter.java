package hr.delmisoft.keycloak.kafka.routing;

import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_TOPIC_ADMIN_EVENTS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_TOPIC_EVENTS;
import static hr.delmisoft.keycloak.kafka.KafkaEventsConst.CFG_TOPIC_OVERRIDES;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfigException;
import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.keycloak.events.EventType;

/**
 * Picks the Kafka topic for an {@link Envelope}.
 *
 * <ul>
 *   <li>User events: the {@code topic-overrides} entry for the event type if there is one, else
 *       {@code topic-events}.</li>
 *   <li>Admin events: {@code topic-admin-events}.</li>
 * </ul>
 *
 * <p>Topics are templates with the placeholders {@value #REALM_ID} and {@value #REALM_NAME}. Templates are
 * parsed and validated once, at construction; an invalid one throws {@link KafkaEventsConfigException}
 * naming the option. The literal parts of a template are what the operator typed, so they must already be a
 * legal Kafka topic name fragment ({@code [a-zA-Z0-9._-]}) — they are never silently rewritten. Only the
 * substituted realm values are sanitized at runtime:
 * <ul>
 *   <li>every code point outside {@code [a-zA-Z0-9._-]} becomes {@code _} (a realm name
 *       {@code "My Realm/ü"} becomes {@code "My_Realm__"});</li>
 *   <li>a missing realm name falls back to the realm id; a missing value becomes {@code _};</li>
 *   <li>the result is truncated to {@value #MAX_TOPIC_LENGTH} characters;</li>
 *   <li>a result Kafka rejects outright — empty, {@code "."} or {@code ".."} (possible only when the template
 *       is nothing but placeholders) — has each character replaced by {@code _} ({@code "_"},
 *       {@code "__"}), the same substitute as for any illegal character.</li>
 * </ul>
 *
 * <p>Nothing is cached: resolving a template is one {@link StringBuilder} pass. Immutable and thread-safe.
 */
public final class TopicRouter {

    /** Placeholder for the realm id (stable across realm renames). */
    public static final String REALM_ID = "{realmId}";

    /** Placeholder for the realm name at event time. */
    public static final String REALM_NAME = "{realmName}";

    /** Kafka's maximum topic name length. */
    public static final int MAX_TOPIC_LENGTH = 249;

    /** Substitute for every character Kafka does not allow in a topic name. */
    static final char SUBSTITUTE = '_';

    private final Template events;
    private final Template adminEvents;
    /** Keyed by {@link EventType#name()}, which is what {@link Envelope.UserData#type()} carries. */
    private final Map<String, Template> overrides;

    /**
     * @param topicEvents      {@code topic-events} template
     * @param topicAdminEvents {@code topic-admin-events} template
     * @param topicOverrides   {@code topic-overrides}: user event type → template
     * @throws KafkaEventsConfigException when a template is invalid
     */
    public TopicRouter(String topicEvents, String topicAdminEvents, Map<EventType, String> topicOverrides) {
        this.events = Template.parse(CFG_TOPIC_EVENTS, topicEvents, topicEvents);
        this.adminEvents = Template.parse(CFG_TOPIC_ADMIN_EVENTS, topicAdminEvents, topicAdminEvents);
        Map<String, Template> parsed = new HashMap<>();
        topicOverrides.forEach((type, topic) -> parsed.put(type.name(),
                Template.parse(CFG_TOPIC_OVERRIDES, type.name() + "=" + topic, topic)));
        this.overrides = Map.copyOf(parsed);
    }

    /** Router for the {@code topic-*} options. */
    public static TopicRouter from(KafkaEventsConfig config) {
        return new TopicRouter(config.topicEvents(), config.topicAdminEvents(), config.topicOverrides());
    }

    /** The topic to publish the envelope to; always a legal Kafka topic name. */
    public String topicFor(Envelope envelope) {
        Template template = switch (envelope.data()) {
            case Envelope.UserData user -> {
                Template override = user.type() == null ? null : overrides.get(user.type());
                yield override != null ? override : events;
            }
            case Envelope.AdminData admin -> adminEvents;
        };
        return template.resolve(envelope.kcrealmid(), envelope.kcrealmname());
    }

    /** A parsed topic template: literal text and placeholders. */
    private static final class Template {

        /** Non-null when the template has no placeholders: the topic is fixed and fully validated. */
        private final String constant;
        /** {@link String} literals and {@link Placeholder}s, in order. */
        private final List<Object> parts;

        private Template(String constant, List<Object> parts) {
            this.constant = constant;
            this.parts = parts;
        }

        /**
         * @param key          option key, for the error message
         * @param displayValue the option value as the operator wrote it, for the error message
         * @param template     the template to parse
         */
        static Template parse(String key, String displayValue, String template) {
            Objects.requireNonNull(template, key);
            if (template.isBlank()) {
                throw invalid(key, displayValue, "topic must not be blank");
            }
            List<Object> parts = new ArrayList<>();
            StringBuilder literal = new StringBuilder();
            int literalLength = 0;
            int i = 0;
            while (i < template.length()) {
                char c = template.charAt(i);
                if (c == '{') {
                    int close = template.indexOf('}', i + 1);
                    int nested = template.indexOf('{', i + 1);
                    if (close < 0 || (nested >= 0 && nested < close)) {
                        throw invalid(key, displayValue, "unbalanced braces: '{' at position " + i
                                + " is not closed by '}'");
                    }
                    Placeholder placeholder = Placeholder.of(template.substring(i, close + 1));
                    if (placeholder == null) {
                        throw invalid(key, displayValue, "unknown placeholder " + template.substring(i, close + 1)
                                + "; supported: " + REALM_ID + ", " + REALM_NAME);
                    }
                    if (!literal.isEmpty()) {
                        parts.add(literal.toString());
                        literal.setLength(0);
                    }
                    parts.add(placeholder);
                    i = close + 1;
                } else if (c == '}') {
                    throw invalid(key, displayValue, "unbalanced braces: '}' at position " + i + " has no '{'");
                } else if (isLegal(c)) {
                    literal.append(c);
                    literalLength++;
                    i++;
                } else {
                    throw invalid(key, displayValue, "illegal character '"
                            + new String(Character.toChars(template.codePointAt(i))) + "' at position " + i
                            + "; topic names allow only [a-zA-Z0-9._-] (realm values substituted for "
                            + REALM_ID + " and " + REALM_NAME + " are sanitized automatically)");
                }
            }
            if (literalLength > MAX_TOPIC_LENGTH) {
                throw invalid(key, displayValue, "topic is longer than " + MAX_TOPIC_LENGTH + " characters");
            }
            if (parts.isEmpty()) {
                String topic = literal.toString();
                if (topic.equals(".") || topic.equals("..")) {
                    throw invalid(key, displayValue, "Kafka does not allow the topic names '.' and '..'");
                }
                return new Template(topic, List.of());
            }
            if (!literal.isEmpty()) {
                parts.add(literal.toString());
            }
            return new Template(null, List.copyOf(parts));
        }

        String resolve(String realmId, String realmName) {
            if (constant != null) {
                return constant;
            }
            StringBuilder topic = new StringBuilder(64);
            for (Object part : parts) {
                if (part instanceof Placeholder placeholder) {
                    String value = placeholder == Placeholder.REALM_ID
                            ? realmId
                            : (realmName != null ? realmName : realmId);
                    appendSanitized(topic, value);
                } else {
                    topic.append((String) part);
                }
            }
            if (topic.length() > MAX_TOPIC_LENGTH) {
                topic.setLength(MAX_TOPIC_LENGTH);
            }
            String result = topic.toString();
            return switch (result) {
                case "" -> String.valueOf(SUBSTITUTE);
                case "." -> String.valueOf(SUBSTITUTE);
                case ".." -> String.valueOf(new char[] {SUBSTITUTE, SUBSTITUTE});
                default -> result;
            };
        }

        /** Appends the value with every code point outside [a-zA-Z0-9._-] replaced by {@link #SUBSTITUTE}. */
        private static void appendSanitized(StringBuilder out, String value) {
            if (value == null) {
                out.append(SUBSTITUTE);
                return;
            }
            value.codePoints().forEach(cp -> out.append(cp < 128 && isLegal((char) cp) ? (char) cp : SUBSTITUTE));
        }

        private static boolean isLegal(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == '-';
        }

        private static KafkaEventsConfigException invalid(String key, String value, String reason) {
            return KafkaEventsConfigException.invalidValue(key, value, reason);
        }
    }

    private enum Placeholder {
        REALM_ID,
        REALM_NAME;

        static Placeholder of(String token) {
            return switch (token) {
                case TopicRouter.REALM_ID -> REALM_ID;
                case TopicRouter.REALM_NAME -> REALM_NAME;
                default -> null;
            };
        }
    }
}
