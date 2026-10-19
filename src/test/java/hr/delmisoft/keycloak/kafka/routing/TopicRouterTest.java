package hr.delmisoft.keycloak.kafka.routing;

import static hr.delmisoft.keycloak.kafka.routing.TestEvents.REALM_ID;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.REALM_NAME;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.adminEvent;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.envelope;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.login;
import static hr.delmisoft.keycloak.kafka.routing.TestEvents.userEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;

import hr.delmisoft.keycloak.kafka.KafkaEventsConst;
import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfig;
import hr.delmisoft.keycloak.kafka.config.KafkaEventsConfigException;
import hr.delmisoft.keycloak.kafka.envelope.Envelope;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeContext;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeMapper;
import hr.delmisoft.keycloak.kafka.envelope.EnvelopeOptions;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.ResourceType;

class TopicRouterTest {

    private static final String LEGAL_TOPIC = "[a-zA-Z0-9._-]{1,249}";

    private static final TopicRouter DEFAULTS = new TopicRouter(
            KafkaEventsConst.DEFAULT_TOPIC_EVENTS, KafkaEventsConst.DEFAULT_TOPIC_ADMIN_EVENTS, Map.of());

    // ---- routing ----

    @Test
    void userEventsGoToTheEventsTopic() {
        assertThat(DEFAULTS.topicFor(envelope(userEvent(EventType.LOGIN))), is("keycloak.events"));
    }

    @Test
    void adminEventsGoToTheAdminEventsTopic() {
        assertThat(DEFAULTS.topicFor(envelope(adminEvent(ResourceType.USER, "users/1"))),
                is("keycloak.admin-events"));
    }

    @Test
    void overrideReplacesTheTopicForItsEventTypeOnly() {
        TopicRouter router = new TopicRouter("keycloak.events", "keycloak.admin-events",
                Map.of(EventType.LOGIN_ERROR, "keycloak.security", EventType.LOGOUT, "logouts.{realmName}"));

        assertThat(router.topicFor(envelope(userEvent(EventType.LOGIN_ERROR))), is("keycloak.security"));
        assertThat(router.topicFor(envelope(userEvent(EventType.LOGOUT))), is("logouts.kafka-demo"));
        assertThat(router.topicFor(envelope(userEvent(EventType.LOGIN))), is("keycloak.events"));
        assertThat(router.topicFor(envelope(adminEvent(ResourceType.USER, "users/1"))),
                is("keycloak.admin-events"));
    }

    @Test
    void placeholdersAreResolvedPerEvent() {
        TopicRouter router = new TopicRouter("kc.{realmName}.events", "kc.{realmId}.admin", Map.of());

        assertThat(router.topicFor(envelope(userEvent(EventType.LOGIN))), is("kc.kafka-demo.events"));
        assertThat(router.topicFor(envelope(adminEvent(ResourceType.USER, "users/1"))),
                is("kc." + REALM_ID + ".admin"));
        assertThat(router.topicFor(login("other-id", "other")), is("kc.other.events"));
    }

    @Test
    void placeholdersCanRepeatAndBeAdjacent() {
        TopicRouter router = new TopicRouter("{realmName}{realmId}-{realmName}", "a", Map.of());

        assertThat(router.topicFor(login("id", "name")), is("nameid-name"));
    }

    @Test
    void realmNameFromTheMapperContextWins() {
        // EnvelopeMapper prefers EnvelopeContext.realmName over Event.getRealmName(); the router uses kcrealmname.
        Envelope envelope = new EnvelopeMapper(EnvelopeOptions.DEFAULTS)
                .map(userEvent(EventType.LOGIN), new EnvelopeContext("urn:x", "renamed"));
        TopicRouter router = new TopicRouter("t.{realmName}", "a", Map.of());

        assertThat(router.topicFor(envelope), is("t.renamed"));
    }

    @Test
    void missingRealmNameFallsBackToRealmId() {
        TopicRouter router = new TopicRouter("t.{realmName}", "a", Map.of());

        assertThat(router.topicFor(login("the-id", null)), is("t.the-id"));
    }

    @Test
    void missingRealmIdBecomesSubstitute() {
        TopicRouter router = new TopicRouter("t.{realmId}", "a", Map.of());

        assertThat(router.topicFor(login(null, "name")), is("t._"));
    }

    @Test
    void fromConfigUsesTheTopicOptions() {
        KafkaEventsConfig config = TestEvents.config("e.{realmName}", "a.{realmName}",
                Map.of(EventType.LOGIN_ERROR, "sec"), Set.of(), Set.of(), Set.of(), Set.of());
        TopicRouter router = TopicRouter.from(config);

        assertThat(router.topicFor(envelope(userEvent(EventType.LOGIN))), is("e." + REALM_NAME));
        assertThat(router.topicFor(envelope(userEvent(EventType.LOGIN_ERROR))), is("sec"));
        assertThat(router.topicFor(envelope(adminEvent(ResourceType.GROUP, "groups/1"))), is("a." + REALM_NAME));
    }

    // ---- runtime sanitization of substituted values ----

    @ParameterizedTest(name = "realm name \"{0}\" → \"{1}\"")
    @CsvSource(delimiter = '|', value = {
            "kafka-demo        | kc.kafka-demo",
            "My Realm          | kc.My_Realm",
            "a/b\\c:d          | kc.a_b_c_d",
            "Zagreb-Ž          | kc.Zagreb-_",
            "日本               | kc.__",
            "emoji😀x          | kc.emoji_x",       // one substitute per code point, not per UTF-16 unit
            "a.b_c-d           | kc.a.b_c-d",
            "'{realmId}'       | kc._realmId_",     // a value is never re-expanded as a template
    })
    void realmNameIsSanitized(String realmName, String expected) {
        TopicRouter router = new TopicRouter("kc.{realmName}", "a", Map.of());

        String topic = router.topicFor(login(REALM_ID, realmName));

        assertThat(topic, is(expected));
        assertThat(topic, matchesPattern(LEGAL_TOPIC));
    }

    @Test
    void realmIdIsSanitizedToo() {
        TopicRouter router = new TopicRouter("kc.{realmId}", "a", Map.of());

        assertThat(router.topicFor(login("id with space", "n")), is("kc.id_with_space"));
    }

    @Test
    void resultIsTruncatedTo249Characters() {
        TopicRouter router = new TopicRouter("prefix.{realmName}", "a", Map.of());

        String topic = router.topicFor(login(REALM_ID, "x".repeat(300)));

        assertThat(topic.length(), is(TopicRouter.MAX_TOPIC_LENGTH));
        assertThat(topic, equalTo("prefix." + "x".repeat(249 - "prefix.".length())));
    }

    @Test
    void truncationCountsSanitizedCharacters() {
        TopicRouter router = new TopicRouter("{realmName}.suffix", "a", Map.of());

        String topic = router.topicFor(login(REALM_ID, "ü".repeat(260)));

        assertThat(topic, equalTo("_".repeat(249)));
    }

    @Test
    void resultOfExactly249CharactersIsKept() {
        TopicRouter router = new TopicRouter("{realmName}", "a", Map.of());
        String name = "n".repeat(249);

        assertThat(router.topicFor(login(REALM_ID, name)), is(name));
    }

    @ParameterizedTest(name = "realm name \"{0}\" → \"{1}\"")
    @CsvSource({
            ".,   _",
            "..,  __",
            "...,  ...",   // legal for Kafka, kept
            "'',  _",      // empty
    })
    void topicsKafkaRejectsAreReplaced(String realmName, String expected) {
        TopicRouter router = new TopicRouter("{realmName}", "a", Map.of());

        assertThat(router.topicFor(login(REALM_ID, realmName)), is(expected));
    }

    @Test
    void dotNamesAreFineInsideALongerTopic() {
        TopicRouter router = new TopicRouter("kc{realmName}", "a", Map.of());

        assertThat(router.topicFor(login(REALM_ID, "..")), is("kc.."));
    }

    // ---- template validation at construction ----

    @ParameterizedTest(name = "\"{0}\"")
    @ValueSource(strings = {
            "keycloak.events",
            "a",
            "...",
            "Keycloak_Events-v1.0",
            "{realmId}",
            "{realmName}",
            "kc.{realmId}.{realmName}.events",
    })
    void validTemplatesAreAccepted(String template) {
        new TopicRouter(template, template, Map.of(EventType.LOGIN, template));
    }

    @Test
    void literalOf249CharactersIsAccepted() {
        String topic = "t".repeat(249);
        TopicRouter router = new TopicRouter(topic, "a", Map.of());

        assertThat(router.topicFor(login(REALM_ID, REALM_NAME)), is(topic));
    }

    @ParameterizedTest(name = "\"{0}\" → {1}")
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
            "\"\"                          | must not be blank",
            "\"   \"                       | must not be blank",
            "kc.{realm}                  | unknown placeholder {realm}",
            "kc.{REALMID}                | unknown placeholder {REALMID}",
            "kc.{}                       | unknown placeholder {}",
            "kc.{realmId                 | '{' at position 3 is not closed",
            "kc.{realm{realmId}          | '{' at position 3 is not closed",
            "kc.realmId}                 | '}' at position 10 has no '{'",
            "kc.}{realmId}               | '}' at position 3 has no '{'",
            "keycloak events             | illegal character ' ' at position 8",
            "keycloak/events             | illegal character '/' at position 8",
            "kc.ž.{realmId}              | illegal character 'ž' at position 3",
            "kc:{realmName}              | illegal character ':' at position 2",
            ".                           | not allow the topic names '.' and '..'",
            "..                          | not allow the topic names '.' and '..'",
    })
    void invalidTopicEventsTemplateFailsFast(String template, String reason) {
        KafkaEventsConfigException e = assertThrows(KafkaEventsConfigException.class,
                () -> new TopicRouter(template, "ok", Map.of()));

        assertThat(e.getMessage(), containsString(
                "Invalid value for spi-events-listener--kafka-events--topic-events: '" + template + "'"));
        assertThat(e.getMessage(), containsString(reason));
    }

    @Test
    void tooLongLiteralFailsFast() {
        String template = "t".repeat(250);
        KafkaEventsConfigException e = assertThrows(KafkaEventsConfigException.class,
                () -> new TopicRouter(template, "ok", Map.of()));

        assertThat(e.getMessage(), containsString("longer than 249 characters"));
    }

    @Test
    void tooLongLiteralAroundPlaceholdersFailsFast() {
        String template = "t".repeat(200) + "{realmId}" + "u".repeat(50);

        assertThrows(KafkaEventsConfigException.class, () -> new TopicRouter(template, "ok", Map.of()));
    }

    @Test
    void invalidAdminTemplateNamesItsOption() {
        KafkaEventsConfigException e = assertThrows(KafkaEventsConfigException.class,
                () -> new TopicRouter("ok", "admin.{user}", Map.of()));

        assertThat(e.getMessage(), containsString(
                "Invalid value for spi-events-listener--kafka-events--topic-admin-events: 'admin.{user}'"));
    }

    @Test
    void invalidOverrideNamesItsOptionAndEntry() {
        KafkaEventsConfigException e = assertThrows(KafkaEventsConfigException.class,
                () -> new TopicRouter("ok", "ok", Map.of(EventType.LOGIN_ERROR, "bad topic")));

        assertThat(e.getMessage(), containsString(
                "Invalid value for spi-events-listener--kafka-events--topic-overrides: 'LOGIN_ERROR=bad topic'"));
        assertThat(e.getMessage(), containsString("illegal character ' '"));
    }
}
