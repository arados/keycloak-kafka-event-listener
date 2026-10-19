package hr.delmisoft.keycloak.kafka.routing;

import static hr.delmisoft.keycloak.kafka.routing.TestEvents.adminEvent;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.userEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.ResourceType;

class EventFilterTest {

    private static final Set<EventType> NONE = Set.of();
    private static final Set<String> NO_RESOURCES = Set.of();

    // ---- user events ----

    @Test
    void noFiltersAcceptEverything() {
        EventFilter filter = new EventFilter(NONE, NONE, NO_RESOURCES, NO_RESOURCES);

        for (EventType type : EventType.values()) {
            assertThat(type.name(), filter.accepts(userEvent(type)), is(true));
        }
        for (ResourceType type : ResourceType.values()) {
            assertThat(type.name(), filter.accepts(adminEvent(type, "x/y")), is(true));
        }
    }

    /** Every include × exclude combination for one event type. */
    @ParameterizedTest(name = "include={0} exclude={1} → {2}")
    @CsvSource({
            // include,  exclude,   accepted
            "'',          '',         true",   // nothing configured
            "LOGIN,       '',         true",   // included
            "LOGOUT,      '',         false",  // include set does not list it
            "'',          LOGIN,      false",  // excluded
            "'',          LOGOUT,     true",   // other type excluded
            "LOGIN,       LOGIN,      false",  // exclude wins over include
            "LOGIN,       LOGOUT,     true",   // included, other type excluded
            "LOGOUT,      LOGOUT,     false",  // neither
    })
    void userEventTypeCombinations(String include, String exclude, boolean accepted) {
        EventFilter filter = new EventFilter(eventTypes(include), eventTypes(exclude), NO_RESOURCES, NO_RESOURCES);

        assertThat(filter.accepts(userEvent(EventType.LOGIN)), is(accepted));
    }

    @Test
    void includeListsSeveralTypes() {
        EventFilter filter = new EventFilter(
                EnumSet.of(EventType.LOGIN, EventType.LOGIN_ERROR), NONE, NO_RESOURCES, NO_RESOURCES);

        assertThat(filter.accepts(userEvent(EventType.LOGIN)), is(true));
        assertThat(filter.accepts(userEvent(EventType.LOGIN_ERROR)), is(true));
        assertThat(filter.accepts(userEvent(EventType.CODE_TO_TOKEN)), is(false));
    }

    @Test
    void userEventFiltersDoNotAffectAdminEvents() {
        EventFilter filter = new EventFilter(
                EnumSet.of(EventType.LOGIN), EnumSet.of(EventType.LOGOUT), NO_RESOURCES, NO_RESOURCES);

        assertThat(filter.accepts(adminEvent(ResourceType.USER, "users/1")), is(true));
    }

    @Test
    void userEventWithoutTypeIsAcceptedOnlyWithoutIncludeSet() {
        Event untyped = userEvent(null);

        assertThat(new EventFilter(NONE, EnumSet.of(EventType.LOGIN), NO_RESOURCES, NO_RESOURCES)
                .accepts(untyped), is(true));
        assertThat(new EventFilter(EnumSet.of(EventType.LOGIN), NONE, NO_RESOURCES, NO_RESOURCES)
                .accepts(untyped), is(false));
    }

    // ---- admin events ----

    @ParameterizedTest(name = "include={0} exclude={1} → {2}")
    @CsvSource({
            // include,  exclude,  accepted
            "'',         '',       true",
            "USER,       '',       true",
            "GROUP,      '',       false",
            "'',         USER,     false",
            "'',         GROUP,    true",
            "USER,       USER,     false",
            "USER,       GROUP,    true",
            "GROUP,      GROUP,    false",
    })
    void adminResourceTypeCombinations(String include, String exclude, boolean accepted) {
        EventFilter filter = new EventFilter(NONE, NONE, strings(include), strings(exclude));

        assertThat(filter.accepts(adminEvent(ResourceType.USER, "users/1")), is(accepted));
    }

    @Test
    void adminResourceTypesAreComparedCaseInsensitively() {
        EventFilter includeLower = new EventFilter(NONE, NONE, Set.of("user", "Realm_Role"), NO_RESOURCES);
        EventFilter excludeLower = new EventFilter(NONE, NONE, NO_RESOURCES, Set.of("user"));

        assertThat(includeLower.accepts(adminEvent(ResourceType.USER, "users/1")), is(true));
        assertThat(includeLower.accepts(adminEvent(ResourceType.REALM_ROLE, "roles/r")), is(true));
        assertThat(includeLower.accepts(adminEvent(ResourceType.GROUP, "groups/1")), is(false));
        assertThat(excludeLower.accepts(adminEvent(ResourceType.USER, "users/1")), is(false));
    }

    @Test
    void customAdminResourceTypeIsMatchedByItsString() {
        AdminEvent custom = adminEvent(null, "custom/1");
        custom.setResourceTypeAsString("my-extension-thing");

        assertThat(new EventFilter(NONE, NONE, Set.of("MY-EXTENSION-THING"), NO_RESOURCES).accepts(custom),
                is(true));
        assertThat(new EventFilter(NONE, NONE, NO_RESOURCES, Set.of("MY-EXTENSION-THING")).accepts(custom),
                is(false));
    }

    @Test
    void adminEventWithoutResourceTypeIsAcceptedOnlyWithoutIncludeSet() {
        AdminEvent untyped = adminEvent(null, null);

        assertThat(new EventFilter(NONE, NONE, NO_RESOURCES, Set.of("USER")).accepts(untyped), is(true));
        assertThat(new EventFilter(NONE, NONE, Set.of("USER"), NO_RESOURCES).accepts(untyped), is(false));
    }

    @Test
    void adminFiltersDoNotAffectUserEvents() {
        EventFilter filter = new EventFilter(NONE, NONE, Set.of("GROUP"), Set.of("USER"));

        assertThat(filter.accepts(userEvent(EventType.LOGIN)), is(true));
    }

    // ---- construction ----

    @Test
    void constructorCopiesItsInputs() {
        Set<EventType> include = EnumSet.of(EventType.LOGIN);
        Set<String> adminExclude = new HashSet<>();
        EventFilter filter = new EventFilter(include, NONE, NO_RESOURCES, adminExclude);

        include.add(EventType.LOGOUT);
        adminExclude.add("USER");

        assertThat(filter.accepts(userEvent(EventType.LOGOUT)), is(false));
        assertThat(filter.accepts(adminEvent(ResourceType.USER, "users/1")), is(true));
    }

    @Test
    void fromConfigUsesTheFilterOptions() {
        KafkaEventsConfig config = TestEvents.config("keycloak.events", "keycloak.admin-events", Map.of(),
                Set.of(EventType.LOGIN, EventType.LOGOUT), Set.of(EventType.LOGOUT), Set.of(), Set.of("GROUP"));

        EventFilter filter = EventFilter.from(config);

        assertThat(filter.accepts(userEvent(EventType.LOGIN)), is(true));
        assertThat(filter.accepts(userEvent(EventType.LOGOUT)), is(false));
        assertThat(filter.accepts(userEvent(EventType.CODE_TO_TOKEN)), is(false));
        assertThat(filter.accepts(adminEvent(ResourceType.USER, "users/1")), is(true));
        assertThat(filter.accepts(adminEvent(ResourceType.GROUP, "groups/1")), is(false));
    }

    private static Set<EventType> eventTypes(String csv) {
        Set<EventType> result = EnumSet.noneOf(EventType.class);
        for (String name : strings(csv)) {
            result.add(EventType.valueOf(name));
        }
        return result;
    }

    private static Set<String> strings(String value) {
        return value == null || value.isEmpty() ? Set.of() : Set.of(value.split("\\|"));
    }
}
