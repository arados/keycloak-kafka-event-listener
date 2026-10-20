package hr.delmisoft.keycloak.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakUriInfo;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.services.DefaultKeycloakTransactionManager;
import org.keycloak.tracing.NoopTracingProvider;
import org.keycloak.tracing.TracingProvider;
import org.keycloak.urls.UrlType;

/**
 * A mocked {@link KeycloakSession} with Keycloak's <em>real</em> {@link DefaultKeycloakTransactionManager}
 * (spied), so begin/commit/rollback and after-completion ordering behave exactly as in the server.
 */
final class KeycloakFixture {

    static final String REALM_ID = "6b1e8f4a-3c2d-4e5f-8a9b-0c1d2e3f4a5b";
    static final String REALM_NAME = "kafka-demo";
    static final String MASTER_ID = "0f9e8d7c-6b5a-4c3d-8e2f-1a0b9c8d7e6f";
    static final String USER_ID = "f3e2d1c0-b9a8-4776-8554-433221100fed";
    static final String SESSION_ID = "9d8c7b6a-5f4e-4d3c-8b2a-1f0e9d8c7b6a";
    static final String BASE_URI = "http://localhost:8080/";
    static final String ISSUER = "http://localhost:8080/realms/kafka-demo";

    final KeycloakSession session = mock(KeycloakSession.class);
    final KeycloakContext context = mock(KeycloakContext.class);
    final KeycloakUriInfo frontend = mock(KeycloakUriInfo.class);
    final RealmProvider realms = mock(RealmProvider.class);
    final RealmModel realm = realm(REALM_ID, REALM_NAME);
    final RealmModel master = realm(MASTER_ID, "master");
    final DefaultKeycloakTransactionManager transactions;

    KeycloakFixture() {
        when(session.getContext()).thenReturn(context);
        when(session.realms()).thenReturn(realms);
        when(session.getProvider(TracingProvider.class)).thenReturn(new NoopTracingProvider());
        when(context.getRealm()).thenReturn(realm);
        when(context.getUri(UrlType.FRONTEND)).thenReturn(frontend);
        when(frontend.getBaseUri()).thenReturn(URI.create(BASE_URI));
        when(realms.getRealm(REALM_ID)).thenReturn(realm);
        when(realms.getRealm(MASTER_ID)).thenReturn(master);
        transactions = spy(new DefaultKeycloakTransactionManager(session));
        when(session.getTransactionManager()).thenReturn(transactions);
    }

    /** Starts the session's transaction, as Keycloak does at the start of a request. */
    KeycloakFixture begin() {
        transactions.begin();
        return this;
    }

    static RealmModel realm(String id, String name) {
        RealmModel realm = mock(RealmModel.class);
        when(realm.getId()).thenReturn(id);
        when(realm.getName()).thenReturn(name);
        return realm;
    }

    static Event userEvent(EventType type) {
        Event event = new Event();
        event.setId("0b6c1e2a-5d4f-4c3b-9a8e-7f6d5c4b3a21");
        event.setType(type);
        event.setTime(1_760_877_296_789L);
        event.setRealmId(REALM_ID);
        event.setRealmName(REALM_NAME);
        event.setClientId("kafka-demo-client");
        event.setUserId(USER_ID);
        event.setSessionId(SESSION_ID);
        event.setIpAddress("192.0.2.10");
        event.setDetails(Map.of("auth_method", "openid-connect", "username", "testuser"));
        return event;
    }

    static AdminEvent adminEvent(ResourceType type, OperationType operation, String resourcePath) {
        AdminEvent event = new AdminEvent();
        event.setId("3e9f4b5d-8a7c-4f6e-ad1b-ac9f8e7d6c54");
        event.setTime(1_760_877_601_234L);
        event.setRealmId(REALM_ID);
        event.setRealmName(REALM_NAME);
        event.setOperationType(operation);
        event.setResourceType(type);
        event.setResourcePath(resourcePath);
        AuthDetails auth = new AuthDetails();
        auth.setRealmId(MASTER_ID);
        auth.setRealmName("master");
        auth.setClientId("admin-cli");
        auth.setUserId("admin-user-id");
        auth.setIpAddress("192.0.2.20");
        event.setAuthDetails(auth);
        event.setRepresentation("{\"username\":\"new-user\",\"enabled\":true}");
        return event;
    }

    static String header(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
