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

Shaded JAR at scaffold time: `target/keycloak-kafka-events-0.1.0-SNAPSHOT.jar` ≈ 20 MB.

## 6. Config plumbing

- `org.keycloak.Config.Scope` (in `keycloak-core`) has `get`, `getInt`, `getLong`, `getBoolean`, `getArray`,
  `scope(...)`, **`getPropertyNames()`**, `root()`. `getPropertyNames()` could enable a generic
  `producer-*` passthrough — verify in M1 that the Quarkus implementation returns env/CLI-provided keys.
- SPI option naming in KC 26: CLI `--spi-events-listener--kafka-events--bootstrap-servers`,
  env `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__BOOTSTRAP_SERVERS` (used in docker-compose.yml).
- Declare options via `EventListenerProviderFactory#getConfigMetadata()` (`ProviderConfigurationBuilder`)
  so they show up in `kc.sh show-config` / server info. Verify current API in M1.

## 7. Reference code on this machine

- `../keycloak-otp` — sibling project by the same author. Patterns to reuse:
  `otp-login` async delivery (`enlistAfterCompletion` → executor → `KeycloakModelUtils.runJobInTransaction`),
  Mockito-based Keycloak model tests, e2e antrun wiring, CI, versioning rules.
- `/Users/arados/Projects/keycloak` — Keycloak source checkout at **26.4.0** (older than our 26.8.1; the outbox
  classes are not in it — use `gh api repos/keycloak/keycloak/contents/<path>?ref=26.8.1` for current code).
