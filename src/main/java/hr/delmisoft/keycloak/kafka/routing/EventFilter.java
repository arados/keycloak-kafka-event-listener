package hr.delmisoft.keycloak.kafka.routing;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

/**
 * Decides whether an event is published at all. Evaluated in {@code onEvent} <em>before</em> mapping, so a
 * rejected event costs a set lookup and nothing else.
 *
 * <p>Semantics, for user event types and admin resource types alike:
 * <ul>
 *   <li>an empty include set accepts everything;</li>
 *   <li>a non-empty include set accepts only what it lists;</li>
 *   <li>the exclude set wins over the include set.</li>
 * </ul>
 * An event without a type (user event) or resource type (admin event) is accepted only when the include
 * set is empty, since it cannot match an include entry.
 *
 * <p>Admin resource types are compared upper case ({@code Locale.ROOT}) against
 * {@link AdminEvent#getResourceTypeAsString()}, which also covers custom resource types set by extensions.
 *
 * <p>Immutable and thread-safe.
 */
public final class EventFilter {

    private final Set<EventType> includeEventTypes;
    private final Set<EventType> excludeEventTypes;
    private final Set<String> includeAdminResourceTypes;
    private final Set<String> excludeAdminResourceTypes;

    /**
     * @param includeEventTypes         user event types to publish; empty = all
     * @param excludeEventTypes         user event types never to publish
     * @param includeAdminResourceTypes admin resource types to publish (any case); empty = all
     * @param excludeAdminResourceTypes admin resource types never to publish (any case)
     */
    public EventFilter(
            Set<EventType> includeEventTypes,
            Set<EventType> excludeEventTypes,
            Set<String> includeAdminResourceTypes,
            Set<String> excludeAdminResourceTypes) {
        this.includeEventTypes = enumCopy(includeEventTypes);
        this.excludeEventTypes = enumCopy(excludeEventTypes);
        this.includeAdminResourceTypes = upperCaseCopy(includeAdminResourceTypes);
        this.excludeAdminResourceTypes = upperCaseCopy(excludeAdminResourceTypes);
    }

    /** Filter for the {@code include-*} / {@code exclude-*} options. */
    public static EventFilter from(KafkaEventsConfig config) {
        return new EventFilter(
                config.includeEventTypes(),
                config.excludeEventTypes(),
                config.includeAdminResourceTypes(),
                config.excludeAdminResourceTypes());
    }

    /** Whether a user event is published. */
    public boolean accepts(Event event) {
        EventType type = event.getType();
        if (type == null) {
            return includeEventTypes.isEmpty();
        }
        return !excludeEventTypes.contains(type)
                && (includeEventTypes.isEmpty() || includeEventTypes.contains(type));
    }

    /** Whether an admin event is published. */
    public boolean accepts(AdminEvent event) {
        String resourceType = event.getResourceTypeAsString();
        if (resourceType == null) {
            return includeAdminResourceTypes.isEmpty();
        }
        String normalized = resourceType.toUpperCase(Locale.ROOT);
        return !excludeAdminResourceTypes.contains(normalized)
                && (includeAdminResourceTypes.isEmpty() || includeAdminResourceTypes.contains(normalized));
    }

    private static Set<EventType> enumCopy(Set<EventType> types) {
        return types.isEmpty() ? EnumSet.noneOf(EventType.class) : EnumSet.copyOf(types);
    }

    private static Set<String> upperCaseCopy(Set<String> types) {
        Set<String> result = new HashSet<>();
        for (String type : types) {
            result.add(type.toUpperCase(Locale.ROOT));
        }
        return Set.copyOf(result);
    }
}
