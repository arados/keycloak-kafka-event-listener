package hr.delmisoft.keycloak.kafka;

import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;

/**
 * Per-session listener. Scaffold only: publishing is milestone M1 in docs/PLAN.md.
 *
 * <p>Design constraints for the implementation (see CLAUDE.md "Key design rules"):
 * never block the request thread on Kafka, publish only after the Keycloak
 * transaction commits, and never let a Kafka failure fail the user's request.
 */
public class KafkaEventListenerProvider implements EventListenerProvider {

    private final KeycloakSession session;

    public KafkaEventListenerProvider(KeycloakSession session) {
        this.session = session;
    }

    @Override
    public void onEvent(Event event) {
        // M1: map to envelope, enlist after-commit publish.
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        // M1: map to envelope (representation only when includeRepresentation), enlist after-commit publish.
    }

    @Override
    public void close() {
    }

    KeycloakSession session() {
        return session;
    }
}
