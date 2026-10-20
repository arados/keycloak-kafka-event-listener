# Keycloak Kafka Event Listener

Keycloak event listener that publishes **user events** and **admin events** to **Apache Kafka** as
CloudEvents JSON.

> **Status: pre-release.** Publishing works end to end and is covered by unit and e2e tests;
> no release has been tagged yet.

## Features

- Drop-in single JAR for Keycloak 26.8.x (`kafka-clients` bundled)
- Never blocks or fails logins: asynchronous, bounded, publishes only after the Keycloak transaction commits
- CloudEvents-shaped, versioned JSON envelope; per-user / per-entity ordering via message keys
- Two topics by default (`keycloak.events`, `keycloak.admin-events`), topic templates and per-type overrides
- TLS / SASL and any other Kafka producer setting
- Safe defaults: no credentials or tokens, unknown event details redacted, admin representations off
- Prometheus metrics through Keycloak's metrics endpoint

Planned: an optional at-least-once mode using Keycloak's durable outbox.

## Quick start (development)

```bash
docker compose up --build -d
# Keycloak: http://localhost:8080/admin (admin/admin), realm "kafka-demo", user testuser/password
# Kafka:    localhost:9094 from the host
mvn verify -P e2e   # full build + end-to-end tests
```

## Installation

1. Copy `keycloak-kafka-event-listener-<version>.jar` into `/opt/keycloak/providers/` and run `kc.sh build`
   (or build your image with it, as the [Dockerfile](Dockerfile) here does).
2. Configure at least the broker, e.g. `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS=kafka:9092`,
   and start Keycloak. All options are runtime options: changing them needs a restart, not a new `kc.sh build`.
3. Enable the listener per realm: *Realm settings → Events → Event listeners* → add `kafka-events` (or
   `"eventsListeners": [..., "kafka-events"]` in the realm JSON / admin REST API).

The listener receives every user and admin event of the realms that enable it. The realm's *Save events*,
*Save admin events* and saved event types only control Keycloak's own event store, not listeners; use the
`include-*`/`exclude-*` options below to choose what goes to Kafka. Admin events carry the representation only
if the realm's *Include representation* is on **and** `include-representation=true`.

At startup Keycloak logs `KC-SERVICES0047: kafka-events ... is implementing the internal SPI eventsListener`.
That is expected for every event listener: Keycloak considers the SPI internal, which is why this plugin
targets one Keycloak minor version at a time.

## Configuration

Options belong to the SPI `events-listener`, provider `kafka-events`. Each option can be set in any of the
usual Keycloak ways; for the option `bootstrap-servers`:

| Where | Form |
|---|---|
| `conf/keycloak.conf` | `spi-events-listener--kafka-events--bootstrap-servers=kafka:9092` |
| command line | `kc.sh start --spi-events-listener--kafka-events--bootstrap-servers=kafka:9092` |
| environment | `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS=kafka:9092` |

Environment names are the option in upper case with `-` → `_`, so the `--` separators become `__`. Use this
`--`/`__` form (Keycloak 26's current one): the older single-separator form is not seen by the `producer-*`
passthrough.

**Unconfigured = disabled, misconfigured = fail fast.** Without `bootstrap-servers` the listener logs one
WARN at startup and publishes nothing, so deploying the JAR unconfigured is harmless. Any invalid value (unknown
event type, bad topic template, unparsable number, unknown Kafka setting, a producer Kafka refuses to build)
stops Keycloak at startup with a message naming the option. Kafka being *unreachable* never fails startup.

| Option | Default | Meaning |
|---|---|---|
| `bootstrap-servers` | — | Kafka bootstrap servers, `host:port[,host:port…]`. Unset or blank → listener disabled. |
| `topic-events` | `keycloak.events` | Topic for user events. A template, see [Topics](#topics-keys-and-ordering). |
| `topic-admin-events` | `keycloak.admin-events` | Topic for admin events. A template. |
| `topic-overrides` | — | Per user event type topics: comma list of `EVENT_TYPE=topic`, e.g. `LOGIN_ERROR=keycloak.security`. Templates allowed. |
| `include-event-types` | — (all) | Comma list of user event types to publish, e.g. `LOGIN,LOGIN_ERROR,LOGOUT`. Names of Keycloak's `EventType`, case-insensitive; an unknown name fails startup. |
| `exclude-event-types` | — | User event types never to publish. Wins over `include-event-types`. |
| `include-admin-resource-types` | — (all) | Comma list of admin resource types to publish, e.g. `USER,CLIENT,REALM_ROLE`. Case-insensitive; custom types allowed. |
| `exclude-admin-resource-types` | — | Admin resource types never to publish. Wins over the include list. |
| `include-ip-address` | `true` | `false` omits the client IP from user events (`data.ipAddress`) and admin events (`data.authDetails.ipAddress`). |
| `include-representation` | `false` | Admin events: include the resource representation (`data.representation`) when Keycloak provides one (realm *Include representation* on). |
| `allowed-detail-keys` | — | Comma list of extra event `details` keys to pass through unredacted (keys set by your extensions). Case-insensitive. Cannot un-redact the built-in denylist. |
| `redacted-detail-keys` | — | Comma list of extra `details` keys to always redact. Case-insensitive; wins over everything. |
| `delivery-mode` | `async` | Only `async` (at-most-once, see [Delivery](#delivery-guarantees)) exists today; `outbox` is planned. |
| `queue-capacity` | `10000` | Maximum number of committed events waiting for the sender thread. Full → new events are dropped and counted. |
| `close-timeout-ms` | `5000` | Shutdown budget for sending what is still queued and closing the producer. |
| `producer-properties-file` | — | Path of a Java `.properties` file (UTF-8) with Kafka producer settings. The place for TLS/SASL settings and secrets. |
| `producer-<kafka-key>` | — | Any Kafka producer setting, with the dots of the Kafka key written as dashes: `producer-linger-ms=50` → `linger.ms=50`. |

### Kafka producer settings

The producer is configured in three layers, later ones winning:

1. the listener's defaults: `acks=all`, `enable.idempotence=true`, `linger.ms=20`, `compression.type=lz4`,
   `max.block.ms=1000`, `request.timeout.ms=10000`, `delivery.timeout.ms=30000`,
   `client.id=keycloak-<hostname>`;
2. the `producer-properties-file`;
3. `producer-*` options, e.g. `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__PRODUCER_COMPRESSION_TYPE=gzip`.

`bootstrap.servers` always comes from `bootstrap-servers`. `key.serializer`/`value.serializer` are rejected
(the listener sets its own). If you set `acks` or `retries` without setting `enable.idempotence`, the
idempotence default is dropped so Kafka can disable it instead of rejecting e.g. `acks=1`. Values are
type-checked at startup, and `delivery.timeout.ms` must be ≥ `linger.ms` + `request.timeout.ms`.

`producer-*` options must name a real Kafka producer setting (an unknown one fails startup); the file may also
contain other keys, e.g. for config providers or custom callback handlers. `ssl.provider` can only be set in
the file: Keycloak treats any SPI option ending in `-provider`, `-enabled` or `-provider-default` as a build-time
option, and a runtime `producer-ssl-provider` would make `kc.sh start --optimized` refuse to start.

**Secrets belong in the file.** Keycloak masks `kc.spi-*` values in `kc.sh show-config`, but environment
variables and command-line arguments are visible in other places (process lists, container inspection). The
listener logs only non-sensitive settings (bootstrap servers and topics at startup; the full configuration at
DEBUG with passwords, JAAS config, tokens and keys masked), and never puts setting values of sensitive keys into
error messages. Kafka's own `ProducerConfig values:` startup log shows password-type settings
(`sasl.jaas.config`, `ssl.*.password`, …) as `[hidden]`.

### TLS and SASL

Example with SASL/SCRAM over TLS. Create a file readable only by the Keycloak user, e.g.
`/opt/keycloak/conf/kafka-producer.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="keycloak" password="change-me";
ssl.truststore.location=/opt/keycloak/conf/kafka-truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=change-me
# mutual TLS instead of / in addition to SASL:
# ssl.keystore.location=/opt/keycloak/conf/kafka-client.p12
# ssl.keystore.type=PKCS12
# ssl.keystore.password=change-me
```

and point the listener at it:

```bash
KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS=kafka-1:9093,kafka-2:9093
KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__PRODUCER_PROPERTIES_FILE=/opt/keycloak/conf/kafka-producer.properties
```

Other mechanisms (PLAIN, OAUTHBEARER, …) work the same way with the corresponding Kafka client settings.
TLS/SASL setups are not yet covered by this project's e2e tests (planned for M2).

## Envelope

Each record value is one [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
event in structured JSON mode (UTF-8). A user event, as published by the demo stack (formatted):

```json
{
  "specversion": "1.0",
  "id": "d88da994-f8f6-470a-9a4a-6c748e426a76",
  "source": "http://localhost:8080/realms/kafka-demo",
  "type": "org.keycloak.event.LOGIN",
  "time": "2026-10-20T07:04:25.167Z",
  "subject": "a4400807-3b54-4920-bb14-c6ae0d838f0f",
  "datacontenttype": "application/json",
  "kcschemaversion": 1,
  "kcrealmid": "a16e407a-2acb-47ef-ab01-9ef85faf4b7f",
  "kcrealmname": "kafka-demo",
  "kckind": "user",
  "data": {
    "type": "LOGIN",
    "clientId": "kafka-demo-client",
    "userId": "a4400807-3b54-4920-bb14-c6ae0d838f0f",
    "sessionId": "UltUQnSlnVX48g4CbXMYA4B_",
    "ipAddress": "192.168.65.1",
    "details": {
      "auth_method": "openid-connect",
      "client_auth_method": "client-secret",
      "grant_type": "password",
      "refresh_token_id": "d802019e-3c9f-7f8b-1190-6a190babb4ea",
      "refresh_token_provider_id": "default",
      "refresh_token_type": "Refresh",
      "scope": "profile email",
      "token_id": "onrtro:7a4a313b-db0a-638f-5469-59a46e84c82a",
      "username": "testuser"
    }
  }
}
```

An admin event:

```json
{
  "specversion": "1.0",
  "id": "2046d585-50dd-4659-9a5f-b51e70cee397",
  "source": "http://localhost:8080/realms/kafka-demo",
  "type": "org.keycloak.admin.USER.CREATE",
  "time": "2026-10-20T07:04:38.568Z",
  "subject": "users/2f0a4de9-f92d-4a41-80e0-1d5d7412caed",
  "datacontenttype": "application/json",
  "kcschemaversion": 1,
  "kcrealmid": "a16e407a-2acb-47ef-ab01-9ef85faf4b7f",
  "kcrealmname": "kafka-demo",
  "kckind": "admin",
  "data": {
    "operationType": "CREATE",
    "resourceType": "USER",
    "resourcePath": "users/2f0a4de9-f92d-4a41-80e0-1d5d7412caed",
    "authDetails": {
      "realmId": "a4403820-0c0f-4541-aa3d-059afa6f272c",
      "realmName": "master",
      "clientId": "ec73c777-4f8b-4b53-bcc4-e119cbb5a464",
      "userId": "355e134f-3a2b-482d-979e-ae725bed1e52",
      "ipAddress": "192.168.65.1"
    }
  }
}
```

| Field | Content |
|---|---|
| `id` | Keycloak's event id (UUID). |
| `source` | The realm's issuer URL, i.e. the `iss` of its tokens; see [below](#source). |
| `type` | `org.keycloak.event.<EventType>` or `org.keycloak.admin.<RESOURCE_TYPE>.<OPERATION>`. |
| `time` | Event time, RFC 3339, UTC, milliseconds. |
| `subject` | User events: the user id. Admin events: the resource path. |
| `kcschemaversion` | Version of the `data`/extension shape; removing or renaming a field means a new version (and a major release). |
| `kcrealmid`, `kcrealmname` | The event's realm. Admin events on realms themselves (realm CREATE/DELETE) carry the admin's realm, as Keycloak records them. |
| `kckind` | `user` or `admin`. |
| `data` | User events: `type`, `clientId`, `userId`, `sessionId`, `ipAddress`, `error`, `details`. Admin events: `operationType`, `resourceType`, `resourcePath`, `authDetails` (who did it: realm, client *internal id*, user id, IP), `error`, `details`, `representation`. |

Fields Keycloak leaves empty are omitted, never written as `null` (e.g. no `error` on success, no `subject` for a
failed login of an unknown user); empty `details` are omitted too. Consumers must treat absent as null and
ignore fields they don't know.

### Headers

| Header | Value |
|---|---|
| `content-type` | `application/cloudevents+json; charset=UTF-8` |
| `kc-kind` | `user` or `admin` |
| `kc-event-type` | the CloudEvents `type`, e.g. `org.keycloak.event.LOGIN` |
| `kc-realm-id` | `kcrealmid` |
| `kc-schema-version` | `kcschemaversion`, e.g. `1` |

They let consumers route and filter without parsing the value.

### `source`

`source` is the realm's issuer URL, `<frontend URL>/realms/<realm name>`, exactly what Keycloak puts into the
realm's tokens as `iss` (the realm's *Frontend URL* if set, else the server's `hostname`). Where no URL can be
determined (no HTTP request and no full-URL `hostname`), it is `urn:keycloak:realm:<realmId>`.

It changes when the hostname or the realm name changes, and with `hostname-strict=false` (e.g. dev mode) it
follows the request's `Host` header. So: **partition, join and deduplicate on `kcrealmid` (and `id`), never on
`source`**, and don't use `source` for authorization decisions; it says where an event came from, it does not
prove it.

## Topics, keys and ordering

User events go to `topic-events`, admin events to `topic-admin-events`; `topic-overrides` sends single user event
types elsewhere. Topic options are templates with two placeholders:

- `{realmId}`: the realm id (stable across renames);
- `{realmName}`: the realm name at event time.

For example `topic-events=keycloak.{realmName}.events`. Characters Kafka doesn't allow in topic names
(anything but `a-z A-Z 0-9 . _ -`) in the *substituted values* become `_`, and the result is cut at 249
characters. Different realm names can therefore map to the same topic, e.g. `My Realm` and `My_Realm` both give
`keycloak.My_Realm.events`; use `{realmId}`, or filter on `kc-realm-id`, when that matters. The literal text of a
template must already be valid (an invalid one fails startup). Topics are not created by the listener: create them,
or let the broker auto-create them. With auto-creation, the first event for a new topic can fail while the broker
creates it, because the producer waits at most `max.block.ms` (1 s by default) for the topic's metadata.

Record keys decide the partition and thus the ordering scope:

| Event | Key | Ordered together |
|---|---|---|
| user event | `<realmId>:<userId>`; without user `<realmId>:<sessionId>`; neither `<realmId>` | all events of one user |
| admin event | `<realmId>:<first two path segments>`, e.g. `<realmId>:users/<id>` for `users/<id>/role-mappings/realm` | all changes to one entity |

There is no ordering across keys or topics.

## Delivery guarantees

Delivery is **at-most-once**:

- **Only committed events.** Events are prepared during the request but handed to the sender only after
  Keycloak's transaction commits; if the request rolls back, they are discarded. Failed logins (`*_ERROR` events)
  are committed in their own transaction by Keycloak and are published even though the request fails.
- **Never blocks logins.** After commit, events go to a bounded in-memory queue (`queue-capacity`). A single
  background thread hands them to the Kafka producer, which batches, compresses, retries, and gives up after
  `delivery.timeout.ms`. Request threads never wait for Kafka: with Kafka down, logins take as long as with
  Kafka up (the e2e test checks < 500 ms; locally ~30 ms either way).
- **Kafka down or slow → queue, then drop.** While the broker is unreachable, the sender waits up to
  `max.block.ms` per event for metadata; the queue absorbs the backlog and, once full, further events are
  dropped. Records Kafka doesn't acknowledge within `delivery.timeout.ms` fail. Both are counted
  ([Metrics](#metrics)) and logged at most once a minute each; the totals are also in the log line the
  listener writes at shutdown. Publishing resumes by itself when Kafka is back.
- **Shutdown** sends what is queued for up to `close-timeout-ms`, then drops the rest (logged). A crash loses
  the queue.

Kafka's client logs its own connection warnings (`org.apache.kafka.clients.NetworkClient`) while a broker is
unreachable; raise that category's level, e.g. `KC_LOG_LEVEL=INFO,org.apache.kafka:error`, if they are too noisy.

## Metrics

The listener registers Micrometer meters in Keycloak's registry. Start Keycloak with `metrics-enabled=true`
(a build option) and they appear on the management interface, `http://<host>:9000/metrics`, next to
Keycloak's own:

| Prometheus name | Type | Labels | Meaning |
|---|---|---|---|
| `keycloak_kafka_events_enqueued_total` | counter | `kind` | Committed events accepted for sending |
| `keycloak_kafka_events_sent_total` | counter | `kind`, `topic` | Events Kafka acknowledged |
| `keycloak_kafka_events_failed_total` | counter | `kind`, `reason` | Send attempts that failed; `reason` is the exception's simple class name, e.g. `TimeoutException` |
| `keycloak_kafka_events_dropped_total` | counter | `kind`, `reason` | Events not published: `queue_full`, `shutdown`, `filtered` (include/exclude options), `error` (the listener failed to prepare the event) |
| `keycloak_kafka_events_queue_size` | gauge | — | Events waiting in the in-memory queue |
| `keycloak_kafka_events_queue_capacity` | gauge | — | `queue-capacity` |

`kind` is `user` or `admin`. Useful alerts: `rate(keycloak_kafka_events_dropped_total{reason="queue_full"}[5m]) > 0`,
`rate(keycloak_kafka_events_failed_total[5m]) > 0`, and `queue_size / queue_capacity` above, say, 0.5.
Without `metrics-enabled` the meters cost next to nothing and are not exposed.

## Privacy

- No credentials, tokens or request bodies are ever exported; the listener never logs event contents.
- `include-ip-address=false` removes client IP addresses.
- **Event `details` are redacted by allowlist**: a value is replaced by `"[redacted]"` (the key stays) unless
  the key is a standard Keycloak detail key (every constant of `org.keycloak.events.Details` plus the few literal
  keys Keycloak core sets) or listed in `allowed-detail-keys`. Unknown keys usually come from extensions and
  may hold anything. This includes the `previous_<attribute>` / `updated_<attribute>` keys Keycloak emits for
  changes to **custom user-profile attributes**: they are redacted unless you allowlist them (e.g.
  `allowed-detail-keys=previous_department,updated_department`). Keys naming credentials (`password`,
  `client_secret`, `otp`, `access_token`, `code_verifier`, …) are always redacted, even if allowlisted;
  `redacted-detail-keys` adds more.
- Admin event **representations are off by default**: they need both the realm's *Include representation* and
  `include-representation=true`. Keycloak strips secrets from representations, but they still contain personal
  data (user attributes, emails).

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

## Compatibility

| Component | Version |
|---|---|
| Keycloak | 26.8.x (built and tested against 26.8.1) |
| Java | 21 (as in the Keycloak image) |
| Kafka client | `kafka-clients` 4.4.0, bundled |
| Kafka brokers | those `kafka-clients` 4.4.0 supports: 2.1 or newer; tested with Apache Kafka 4.3.1 (KRaft) |

The JAR does not relocate `kafka-clients`; another provider bundling a different `kafka-clients` version in the
same Keycloak may clash.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
