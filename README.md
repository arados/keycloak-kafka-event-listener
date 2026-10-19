# Keycloak Kafka Event Listener

Keycloak event listener that publishes **user events** and **admin events** to **Apache Kafka**.

> **Status: work in progress (scaffold).** The provider registers in Keycloak but does not publish yet.
> See [docs/PLAN.md](docs/PLAN.md) for the design and roadmap.

## Planned features

- Drop-in single JAR for Keycloak 26.8.x (`kafka-clients` bundled)
- Never blocks or fails logins: asynchronous, bounded, publishes only after the Keycloak transaction commits
- CloudEvents-shaped, versioned JSON envelope; per-user / per-entity ordering via message keys
- Two topics by default (`keycloak.events`, `keycloak.admin-events`), topic templates and per-type overrides
- TLS / SASL and arbitrary producer properties
- Safe defaults: no credentials, admin representations off unless explicitly enabled
- Optional at-least-once mode using Keycloak's durable outbox

## Quick start (development)

```bash
docker compose up --build -d
# Keycloak: http://localhost:8080/admin (admin/admin), realm "kafka-demo", user testuser/password
# Kafka:    localhost:9094 from the host
mvn verify -P e2e   # full build + end-to-end tests
```

## Installation (once released)

1. Copy `keycloak-kafka-event-listener-<version>.jar` into `/opt/keycloak/providers/` and run `kc.sh build`.
2. Configure the broker, e.g. `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS=kafka:9092`.
3. In the admin console: *Realm settings → Events → Event listeners* → add `kafka-events`.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
