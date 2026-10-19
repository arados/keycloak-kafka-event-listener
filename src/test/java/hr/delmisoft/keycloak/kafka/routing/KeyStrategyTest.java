package hr.delmisoft.keycloak.kafka.routing;

import static hr.delmisoft.keycloak.kafka.routing.TestEvents.REALM_ID;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.SESSION_ID;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.USER_ID;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.adminEvent;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.envelope;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.userEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.ResourceType;

class KeyStrategyTest {

    private final KeyStrategy keys = new KeyStrategy();

    // ---- user events ----

    @Test
    void userEventIsKeyedByUser() {
        assertThat(keys.keyFor(envelope(userEvent(EventType.LOGIN))), is(REALM_ID + ":" + USER_ID));
    }

    @Test
    void userEventWithoutUserIsKeyedBySession() {
        Event event = userEvent(EventType.LOGIN_ERROR);
        event.setUserId(null);

        assertThat(keys.keyFor(envelope(event)), is(REALM_ID + ":" + SESSION_ID));
    }

    @Test
    void userEventWithEmptyUserIsKeyedBySession() {
        Event event = userEvent(EventType.LOGIN_ERROR);
        event.setUserId("");

        assertThat(keys.keyFor(envelope(event)), is(REALM_ID + ":" + SESSION_ID));
    }

    @Test
    void userEventWithoutUserOrSessionIsKeyedByRealm() {
        Event event = userEvent(EventType.LOGIN_ERROR);
        event.setUserId(null);
        event.setSessionId(null);

        assertThat(keys.keyFor(envelope(event)), is(REALM_ID));
    }

    @Test
    void userEventWithEmptySessionIsKeyedByRealm() {
        Event event = userEvent(EventType.LOGIN_ERROR);
        event.setUserId(null);
        event.setSessionId("");

        assertThat(keys.keyFor(envelope(event)), is(REALM_ID));
    }

    @Test
    void userAndAnonymousSessionKeysDiffer() {
        Event withUser = userEvent(EventType.LOGIN);
        Event withoutUser = userEvent(EventType.LOGIN_ERROR);
        withoutUser.setUserId(null);

        assertThat(keys.keyFor(envelope(withUser)).equals(keys.keyFor(envelope(withoutUser))), is(false));
    }

    // ---- admin events ----

    @ParameterizedTest(name = "\"{0}\" → \"{1}\"")
    @CsvSource(delimiter = '|', value = {
            "users/u1                                    | users/u1",
            "users/u1/role-mappings/realm                | users/u1",
            "users/u1/groups/g1                          | users/u1",
            "clients/c1/protocol-mappers/models/m1       | clients/c1",
            "authentication/flows                        | authentication/flows",
            "events/config                               | events/config",
            "users                                       | users",
    })
    void adminEventIsKeyedByTheFirstTwoPathSegments(String path, String expectedScope) {
        AdminEvent event = adminEvent(ResourceType.USER, path);

        assertThat(keys.keyFor(envelope(event)), is(REALM_ID + ":" + expectedScope));
    }

    @Test
    void allChangesToOneEntityShareAKey() {
        String user = keys.keyFor(envelope(adminEvent(ResourceType.USER, "users/u1")));
        String roles = keys.keyFor(envelope(adminEvent(ResourceType.REALM_ROLE_MAPPING,
                "users/u1/role-mappings/realm")));

        assertThat(roles, is(user));
    }

    @Test
    void adminEventWithoutPathIsKeyedByRealm() {
        assertThat(keys.keyFor(envelope(adminEvent(ResourceType.REALM, null))), is(REALM_ID));
    }

    @Test
    void adminEventWithEmptyPathIsKeyedByRealm() {
        assertThat(keys.keyFor(envelope(adminEvent(ResourceType.REALM, ""))), is(REALM_ID));
    }

    // ---- realm id ----

    @Test
    void missingRealmIdIsTreatedAsEmpty() {
        Event event = userEvent(EventType.LOGIN);
        event.setRealmId(null);

        assertThat(keys.keyFor(envelope(event)), is(":" + USER_ID));
    }

    @Test
    void keysOfDifferentRealmsDiffer() {
        Event other = userEvent(EventType.LOGIN);
        other.setRealmId("other-realm");

        assertThat(keys.keyFor(envelope(other)), is("other-realm:" + USER_ID));
    }
}
