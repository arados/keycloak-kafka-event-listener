package hr.delmisoft.keycloak.kafka;

import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

/**
 * Registers the {@value KafkaEventsConst#PROVIDER_ID} event listener.
 *
 * <p>Scaffold only: configuration parsing and the shared {@code KafkaProducer}
 * lifecycle (created in {@link #postInit}, closed in {@link #close}) are
 * milestone M1 in docs/PLAN.md.
 */
public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new KafkaEventListenerProvider(session);
    }

    @Override
    public void init(Config.Scope config) {
        // M1: read KafkaEventsConst.CFG_* keys, fail fast on invalid config.
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // M1: create the single shared KafkaProducer here.
    }

    @Override
    public void close() {
        // M1: flush and close the shared KafkaProducer with a bounded timeout.
    }

    @Override
    public String getId() {
        return KafkaEventsConst.PROVIDER_ID;
    }
}
