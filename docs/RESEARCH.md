# Research notes

Findings gathered on 2026-10-19 before the project was created. They explain *why* the plan in
[PLAN.md](PLAN.md) looks the way it does. Re-verify anything time-sensitive (versions, issue status)
before relying on it; add new findings here with a date.

## 1. Existing Kafka listeners for Keycloak (why build another one)

| Project | Keycloak support | Topic model | Notes |
|---|---|---|---|
| [SnuK87/keycloak-kafka](https://github.com/SnuK87/keycloak-kafka) | README: tested 19.x / 21.x ("21+") | one events topic + optional admin topic, event-type filter | Most known; last push Sep 2024; NPE at startup when deployed without config; 1.4.0 changelog: "stops blocking event sends from affecting Keycloak when Kafka is down" (it used to block). |
| [playa-ru/keycloak-kafka-provider](https://github.com/playa-ru/keycloak-kafka-provider) | 26.5.0 on [Maven Central](https://central.sonatype.com/artifact/ru.playa.keycloak/keycloak-kafka-provider) | login topic + admin topic; async default | Russian-only docs, 2 stars; config via properties file + env. |
| [mstickel/…-publisher-event-listener-provider](https://github.com/mstickel/keycloak-kafka-publisher-event-listener-provider) | — | single topic | `REGISTER` events only. |
| DZone tutorial, juliuskrah, dann41 | KC 17-era demos | DZone = topic per event type | Not maintained. |
| [p2-inc/keycloak-events](https://github.com/p2-inc/keycloak-events) | tracks latest | HTTP webhooks (not Kafka) | Maintained; retries/DLQ claimed by vendor. |

Gap we fill: maintained + Keycloak-26-tested + documented + stable versioned envelope + configurable
routing + SASL/mTLS + explicit delivery-guarantee story (+ optional at-least-once outbox later).

## 2. Keycloak upstream state (don't duplicate, stay compatible)

- [#45303 "External events store"](https://github.com/keycloak/keycloak/issues/45303) — by project lead
  stianst, milestone "Future", placeholder. States: *event listeners are not transactional; an event can be
  lost if the server dies right after the transaction completes.*
- [#52616 versioned event-export envelope + destination SPI](https://github.com/keycloak/keycloak/issues/52616)
  — external proposal (2026-09-10), only triaged. Requirements worth mirroring in our design:
  stable event id across retries/duplicates; capture in the dispatching transaction, publish only committed;
  filtering before capture; `includeRepresentation=false` is a hard ceiling; never export secrets/tokens/request
  bodies; destination failures isolated; at-least-once; no exactly-once / global ordering. CloudEvents 1.0 is a
  *candidate* envelope.
- [#41065 built-in webhooks](https://github.com/keycloak/keycloak/issues/41065) (27 👍) and
  [PR #48168 "event-hook system"](https://github.com/keycloak/keycloak/pull/48168) (17.6k lines, no maintainer
  review since Apr 2026 — "no capacity").
- Discussions: [#44261 audit storage / Kafka](https://github.com/keycloak/keycloak/discussions/44261),
  [#25133 CloudEvents listener](https://github.com/keycloak/keycloak/discussions/25133) (proposes a CloudEvents
  field mapping: `type`=`org.keycloak.<EventType>`, `subject`=userId, `source`=realm URL, `time` RFC 3339).

Takeaway: nothing official is coming soon. Using a CloudEvents-shaped envelope keeps us close to whatever
upstream eventually picks.

## 3. How Keycloak dispatches events (26.8.x)

Source: `server-spi-private/src/main/java/org/keycloak/events/EventBuilder.java`.

- `success()` → `send(event, storeImmediately ?: false)` → listeners' `onEvent(Event)` called **synchronously
  on the request thread, inside the request transaction** (before commit).
- `error()` → `send(event, storeImmediately ?: true)` → `KeycloakModelUtils.runJobInTransaction(...)` with a
  **new inner session/transaction**; listeners are re-resolved for that inner session.
- Each listener call is wrapped in `try/catch(Throwable)` and logged ("Failed to send type to …") — a throwing
  listener does not fail the request, but a *blocking* listener blocks it.
- The realm's "enabled event types" filter applies **only to the event store**, not to listeners: listeners
  receive every event type. Our own type filter is therefore needed.
- `org.keycloak.events.EventListenerTransaction` (server-spi-private) is a ready-made
  `AbstractKeycloakTransaction` that buffers `Event`s / `AdminEvent`s and hands them to consumers on
  `commitImpl()`, clearing on rollback. Built-in `EmailEventListenerProvider` uses this pattern via
  `session.getTransactionManager().enlistAfterCompletion(tx)`. Constructor:
  `EventListenerTransaction(BiConsumer<AdminEvent, Boolean>, Consumer<Event>)`.
- Admin events: `onEvent(AdminEvent, boolean includeRepresentation)`.

## 4. Keycloak's generic durable outbox (for the optional at-least-once mode)

Added with SSF in 26.7.0 ([PR #48256](https://github.com/keycloak/keycloak/pull/48256), maintainer
Thomas Darimont). Package `org.keycloak.events.outbox` in **`model/jpa`** — internal, *not* a public SPI.

- Table `OUTBOX_ENTRY` (`jpa-changelog-26.7.0.xml`), entity `OutboxEntryEntity`; rows scoped by `entryKind`
  (javadoc names "ssf" and "webhook"), `entryType`, `realmId`, `ownerId`, `containerId`, `correlationId`,
  opaque `payload`/`metadata` text, status PENDING/HELD/DELIVERED/DEAD_LETTER, attempts, `nextAttemptAt`.
- `OutboxStore(KeycloakSession)`: `enqueuePending(...)`, `lockDueForDrain(kind, limit)` (FOR UPDATE SKIP
  LOCKED), `markDelivered`, `recordFailure`, `markDeadLetter`, retention purges.
- `OutboxDrainerTask(OutboxConfig, OutboxDeliveryHandler, Function<KeycloakSession,OutboxStore>)` — a
  `ScheduledTask`, wrapped in `ClusterAwareScheduledTaskRunner` so one node drains per interval; backoff,
  dead-letter, retention.
- `OutboxDeliveryHandler`: `String entryKind()`, `OutboxDeliveryResult deliver(KeycloakSession, OutboxEntryEntity)`.
  Javadoc: *returning `delivered()` as soon as the payload has been handed off (e.g. enqueued in an external
  broker)* — i.e. a Kafka handler = `producer.send(...).get(timeout)` → `delivered()`.
- Open questions to verify before M3: is the table created regardless of the SSF feature flag; how SSF
  constructs/schedules its drainer (copy that); signature stability between 26.7 → 26.8 → next.

## 5. Keycloak runtime libraries (what we must / must not bundle)

Checked in `quay.io/keycloak/keycloak:26.8.1`, `/opt/keycloak/lib/lib/main`:

| Library | Shipped by Keycloak? | Our action |
|---|---|---|
| `kafka-clients` | **no** | bundle (shade) |
| `zstd-jni`, `lz4-java`, `snappy-java` | **no** | bundled today (they are kafka-clients runtime deps); ~most of the 20 MB JAR — see PLAN open decision D4 |
| `slf4j-api` | yes, 2.0.18 (+ `slf4j-jboss-logmanager`) | do **not** bundle (kafka-clients wants 1.7.36; 2.x API is compatible for its usage) |
| `jackson-databind` | yes, 2.21.7 | use Keycloak's (`org.keycloak.util.JsonSerialization`), don't bundle |
| `micrometer-core` | yes, 1.17.1 | use for metrics (M2), don't bundle |

Re-check on every Keycloak bump (`bump-keycloak` skill).

Shaded JAR at scaffold time: `target/keycloak-kafka-event-listener-0.1.0-SNAPSHOT.jar` ≈ 20 MB.

## 6. Config plumbing

- `org.keycloak.Config.Scope` (in `keycloak-core`) has `get`, `getInt`, `getLong`, `getBoolean`, `getArray`,
  `scope(...)`, **`getPropertyNames()`**, `root()`.
- SPI option naming in KC 26: CLI `--spi-events-listener--kafka-events--bootstrap-servers`,
  env `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS` (used in docker-compose.yml),
  keycloak.conf `spi-events-listener--kafka-events--bootstrap-servers=…`.

### 6.1 `getPropertyNames()` under Quarkus (verified 2026-10-19, Keycloak 26.8.1)

Source: `quarkus/runtime/.../configuration/MicroProfileConfigProvider.java` (`MicroProfileScope`),
`KcEnvConfigSource.java`, `ConfigArgsConfigSource.java`, `mappers/PropertyMappers.java`, `cli/Picocli.java`
at tag 26.8.1. Confirmed empirically by logging `getPropertyNames()` + `get()` from our factory's `init` in
the compose stack (`start-dev`) and in the built image with `start --optimized`.

- Returns **full SmallRye keys**, not suffixes: `kc.spi-events-listener--kafka-events--producer-linger-ms`.
  Strip the prefix `kc.spi-events-listener--kafka-events--` ourselves.
- Implementation: filters `SmallRyeConfig.getPropertyNames()` by the **new `--` separator prefix only**.
- Sources that show up (all verified): env `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__PRODUCER_LINGER_MS`
  (`KcEnvConfigSource` rewrites `KC_*` to `kc.` + lowercase + `_`→`-`, so `__`→`--`), CLI
  `--spi-events-listener--kafka-events--producer-compression-type=lz4`, keycloak.conf
  `spi-events-listener--kafka-events--producer-batch-size=1234`.
- **Legacy single-separator form is invisible to enumeration**: env `KC_SPI_EVENTS_LISTENER_KAFKA_EVENTS_PRODUCER_ACKS`
  is readable via `get("producer-acks")` (falls back to the old name) but is **not** in `getPropertyNames()`.
  → passthrough only supports the `--` form; document it.
- `KCKEY_<X>=spi-events-listener--kafka-events--producer-client.id` + `KC_<X>=…` produces a dotted key that
  *is* listed, but `get("producer-client.id")` returns null (key is dash-cased before lookup). Don't support
  dotted keys; use the dash form.
- Kafka 4.4.0 `ProducerConfig.configNames()`: all 120 keys match `[a-z0-9.]+`, so `producer-a-b-c` →
  `a.b.c` (dash→dot) is lossless.
- **Build-time suffix trap.** `PropertyMappers.isSpiBuildTimeProperty` treats any `kc.spi-*` key ending in
  `-provider`, `-enabled` or `-provider-default` as a **build-time** option. Setting such a key at runtime on an
  optimized image makes `kc.sh start --optimized` **exit 2**: *"The following build time options have values
  that differ from what is persisted … kc.spi-events-listener--kafka-events--producer-ssl-provider"*.
  Consequences: (1) Kafka `ssl.provider` (the only producer key with such a suffix) cannot go through the
  passthrough — it must use `producer-properties-file`; (2) **none of our own option names may end in
  `-enabled`, `-provider` or `-provider-default`** (e.g. use `metrics=true`, not `metrics-enabled`).
- `show-config` prints every `kc.spi-*` key with the value masked, declared or not.

### 6.2 `getConfigMetadata()` (verified 2026-10-19, Keycloak 26.8.1)

- `ProviderFactory#getConfigMetadata()` is a default method returning an empty list. Nothing validates SPI
  options against it: unknown `kc.spi-*` keys pass straight through (no mapper → `context.proceed`).
- Generic callers in 26.8.1: only `AuthenticationManagementResource` (authenticator config) and
  `ServerInfoAdminResource`, which renders config only for `ConfiguredProvider` implementations — an
  `EventListenerProviderFactory` isn't one, so our metadata isn't shown anywhere.
- Still worth implementing (`ProviderConfigurationBuilder`): it documents the options in code, costs nothing,
  and future Keycloak versions may surface it. It is not required for options to work.

### 6.3 Kafka producer default pitfalls (verified 2026-10-19, kafka-clients 4.4.0 source)

- `KafkaProducer.configureDeliveryTimeout`: if `delivery.timeout.ms < linger.ms + request.timeout.ms` and
  `delivery.timeout.ms` was set **explicitly**, the constructor throws `ConfigException` (otherwise it silently
  raises it). With `request.timeout.ms` at Kafka's default 30000, an explicit `delivery.timeout.ms=30000` plus
  `linger.ms=20` fails → we also default `request.timeout.ms=10000` and check the sum at config time.
- `ProducerConfig.postProcessAndValidateIdempotenceConfigs`: with `enable.idempotence` **explicitly** true,
  `acks≠all` or `retries=0` throw; with Kafka's implicit default (true) they just disable idempotence.
  `max.in.flight.requests.per.connection > 5` throws either way. → our explicit `enable.idempotence=true`
  default is dropped when the user sets `acks`/`retries` without setting idempotence.
- `ProducerConfig.configNames()` includes `config.providers`; `ConfigDef.parseType` + the key's validator can
  type-check a single value without building a `ProducerConfig` (which would log all values).

## 7. Reference code on this machine

- `../keycloak-otp` — sibling project by the same author. Patterns to reuse:
  `otp-login` async delivery (`enlistAfterCompletion` → executor → `KeycloakModelUtils.runJobInTransaction`),
  Mockito-based Keycloak model tests, e2e antrun wiring, CI, versioning rules.
- `/Users/arados/Projects/keycloak` — Keycloak source checkout at **26.4.0** (older than our 26.8.1; the outbox
  classes are not in it — use `gh api repos/keycloak/keycloak/contents/<path>?ref=26.8.1` for current code).
