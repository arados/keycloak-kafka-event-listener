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

## Compression

The JAR bundles `kafka-clients` and the **lz4** codec only (pure Java; no native libraries are shipped or
extracted). `lz4` is the default `compression.type`; `gzip` and `none` need nothing extra either.

`zstd` and `snappy` are not bundled (they would nearly double the JAR and unpack native libraries into
`java.io.tmpdir` at runtime). To use them, add the codec JAR that matches the bundled kafka-clients 4.4.0 to
`/opt/keycloak/providers/` next to this JAR and run `kc.sh build`:

| `compression.type` | JAR to add |
|---|---|
| `zstd` | [`com.github.luben:zstd-jni:1.5.7-17`](https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.7-17/zstd-jni-1.5.7-17.jar) |
| `snappy` | [`org.xerial.snappy:snappy-java:1.1.10.7`](https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar) |

These extract a native library to `java.io.tmpdir` on first use, so it must be writable and not mounted
`noexec`. Setting `zstd` or `snappy` without the JAR makes the listener fail at startup.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
