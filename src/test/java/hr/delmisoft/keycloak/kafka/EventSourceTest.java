package hr.delmisoft.keycloak.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ContextNotActiveException;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakUriInfo;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.urls.UrlType;

/** Decision D2: {@code source} = realm issuer, fallback {@code urn:keycloak:realm:<realmId>}. */
class EventSourceTest {

    private static final String REALM_ID = "6b1e8f4a-3c2d-4e5f-8a9b-0c1d2e3f4a5b";
    private static final String MASTER_ID = "0f9e8d7c-6b5a-4c3d-8e2f-1a0b9c8d7e6f";

    private final KeycloakSession session = mock(KeycloakSession.class);
    private final KeycloakContext context = mock(KeycloakContext.class);
    private final KeycloakUriInfo frontend = mock(KeycloakUriInfo.class);
    private final RealmProvider realms = mock(RealmProvider.class);
    private final RealmModel demo = realm(REALM_ID, "kafka-demo");
    private final RealmModel master = realm(MASTER_ID, "master");

    @BeforeEach
    void setUp() {
        when(session.getContext()).thenReturn(context);
        when(session.realms()).thenReturn(realms);
        when(context.getUri(UrlType.FRONTEND)).thenReturn(frontend);
        when(frontend.getBaseUri()).thenReturn(URI.create("http://localhost:8080/"));
        when(realms.getRealm(REALM_ID)).thenReturn(demo);
        when(realms.getRealm(MASTER_ID)).thenReturn(master);
    }

    @Test
    void eventInTheContextRealmGetsTheTokenIssuer() {
        when(context.getRealm()).thenReturn(demo);

        var resolved = EventSource.resolve(session, REALM_ID, "kafka-demo");

        assertThat(resolved.source(), equalTo("http://localhost:8080/realms/kafka-demo"));
        assertThat(resolved.realmName(), equalTo("kafka-demo"));
        // Keycloak's hostname provider already applied the context realm's frontendUrl to the base URI.
        verify(demo, never()).getAttribute(EventSource.FRONTEND_URL_ATTRIBUTE);
        verify(realms, never()).getRealm(REALM_ID);
    }

    @Test
    void contextPathAndFrontendBaseAreKept() {
        when(context.getRealm()).thenReturn(demo);
        when(frontend.getBaseUri()).thenReturn(URI.create("https://sso.example.com/auth/"));

        assertThat(EventSource.resolve(session, REALM_ID, "kafka-demo").source(),
                equalTo("https://sso.example.com/auth/realms/kafka-demo"));
    }

    @Test
    void realmNameIsPercentEncodedLikeKeycloakDoes() {
        RealmModel spaced = realm(REALM_ID, "My Realm");
        when(context.getRealm()).thenReturn(spaced);

        assertThat(EventSource.resolve(session, REALM_ID, "My Realm").source(),
                equalTo("http://localhost:8080/realms/My%20Realm"));
    }

    @Test
    void missingRealmNameIsReadFromTheRealm() {
        when(context.getRealm()).thenReturn(demo);

        var resolved = EventSource.resolve(session, REALM_ID, null);

        assertThat(resolved.source(), equalTo("http://localhost:8080/realms/kafka-demo"));
        assertThat(resolved.realmName(), equalTo("kafka-demo"));
    }

    @Test
    void realmCreateCarriesTheAdminRealmWhichIsAlsoTheContextRealm() {
        // RealmsAdminResource.importRealm: event realm = admin's realm; RealmManager restores the context realm.
        when(context.getRealm()).thenReturn(master);

        assertThat(EventSource.resolve(session, MASTER_ID, "master").source(),
                equalTo("http://localhost:8080/realms/master"));
    }

    @Test
    void realmDeleteCarriesTheAdminRealmWhileTheContextIsTheDeletedRealm() {
        when(context.getRealm()).thenReturn(demo);

        assertThat(EventSource.resolve(session, MASTER_ID, "master").source(),
                equalTo("http://localhost:8080/realms/master"));
    }

    @Test
    void eventRealmFrontendUrlWinsWhenTheContextIsAnotherRealm() {
        when(context.getRealm()).thenReturn(demo);
        when(master.getAttribute(EventSource.FRONTEND_URL_ATTRIBUTE)).thenReturn("https://admin.example.com:443/kc");

        assertThat(EventSource.resolve(session, MASTER_ID, "master").source(),
                equalTo("https://admin.example.com/kc/realms/master"));
    }

    @Test
    void eventRealmFrontendUrlIsUsedWithoutAContextRealm() {
        when(context.getRealm()).thenReturn(null);
        when(demo.getAttribute(EventSource.FRONTEND_URL_ATTRIBUTE)).thenReturn("https://sso.example.com:8443");

        assertThat(EventSource.resolve(session, REALM_ID, "kafka-demo").source(),
                equalTo("https://sso.example.com:8443/realms/kafka-demo"));
    }

    @Test
    void contextBaseIsUsedWithoutAContextRealmOrFrontendUrl() {
        when(context.getRealm()).thenReturn(null);

        assertThat(EventSource.resolve(session, REALM_ID, "kafka-demo").source(),
                equalTo("http://localhost:8080/realms/kafka-demo"));
    }

    @Test
    void invalidEventRealmFrontendUrlIsIgnoredLikeKeycloakDoes() {
        when(context.getRealm()).thenReturn(demo);
        when(master.getAttribute(EventSource.FRONTEND_URL_ATTRIBUTE)).thenReturn("ftp://nope");

        assertThat(EventSource.resolve(session, MASTER_ID, "master").source(),
                equalTo("http://localhost:8080/realms/master"));
    }

    @Test
    void anotherRealmsFrontendUrlInTheContextBaseFallsBackToTheUrn() {
        // Realm DELETE of a realm with its own frontendUrl: the context base is that realm's URL, not master's.
        when(context.getRealm()).thenReturn(demo);
        when(demo.getAttribute(EventSource.FRONTEND_URL_ATTRIBUTE)).thenReturn("https://demo.example.com");

        assertThat(EventSource.resolve(session, MASTER_ID, "master").source(),
                equalTo("urn:keycloak:realm:" + MASTER_ID));
    }

    @Test
    void noHttpContextFallsBackToTheUrn() {
        when(context.getRealm()).thenReturn(demo);
        when(context.getUri(UrlType.FRONTEND)).thenThrow(new ContextNotActiveException());

        var resolved = EventSource.resolve(session, REALM_ID, "kafka-demo");

        assertThat(resolved.source(), equalTo("urn:keycloak:realm:" + REALM_ID));
        assertThat(resolved.realmName(), equalTo("kafka-demo"));
    }

    @Test
    void unknownRealmWithoutNameFallsBackToTheUrn() {
        when(context.getRealm()).thenReturn(demo);

        var resolved = EventSource.resolve(session, "gone", null);

        assertThat(resolved.source(), equalTo("urn:keycloak:realm:gone"));
        assertThat(resolved.realmName(), nullValue());
    }

    @Test
    void failingRealmLookupFallsBackToTheUrn() {
        when(context.getRealm()).thenReturn(demo);
        when(realms.getRealm(MASTER_ID)).thenThrow(new IllegalStateException("closed"));

        assertThat(EventSource.resolve(session, MASTER_ID, "master").source(),
                equalTo("urn:keycloak:realm:" + MASTER_ID));
    }

    @Test
    void urnFallbackEncodesUnusualRealmIds() {
        assertThat(EventSource.fallback(REALM_ID), equalTo("urn:keycloak:realm:" + REALM_ID));
        assertThat(EventSource.fallback("my realm/ü"), equalTo("urn:keycloak:realm:my%20realm%2F%C3%BC"));
        assertThat(EventSource.fallback(null), equalTo("urn:keycloak:realm"));
        assertThat(EventSource.resolve(session, null, "x").source(), equalTo("urn:keycloak:realm"));
    }

    private static RealmModel realm(String id, String name) {
        RealmModel realm = mock(RealmModel.class);
        when(realm.getId()).thenReturn(id);
        when(realm.getName()).thenReturn(name);
        return realm;
    }
}
