package hr.delmisoft.keycloak.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakSession;

class KafkaEventListenerProviderFactoryTest {

    private final KafkaEventListenerProviderFactory factory = new KafkaEventListenerProviderFactory();

    @Test
    void providerIdIsStable() {
        // Realms reference the listener by this id; changing it silently disables publishing.
        assertThat(factory.getId(), equalTo("kafka-events"));
    }

    @Test
    void createReturnsSessionScopedProvider() {
        KeycloakSession session = mock(KeycloakSession.class);

        var provider = factory.create(session);

        assertThat(provider, instanceOf(KafkaEventListenerProvider.class));
        assertThat(((KafkaEventListenerProvider) provider).session(), sameInstance(session));
    }
}
