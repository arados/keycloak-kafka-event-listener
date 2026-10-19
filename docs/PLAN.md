# Implementation plan

This is the working plan. The SessionStart hook surfaces the **first unchecked `- [ ]` task** to every new
session, so keep the checklists honest: tick a task in the same PR that completes it, and add new tasks
where they belong rather than at the end. Background and sources: [RESEARCH.md](RESEARCH.md).

## Goals

1. Publish Keycloak **user events** and **admin events** to Kafka from a drop-in provider JAR.
2. **Never affect the auth flow**: no request-thread blocking on Kafka, no failed logins because Kafka is
   down/slow/misconfigured, bounded memory.
3. Publish **only committed** events (no events for rolled-back transactions).
4. A **stable, versioned, CloudEvents-shaped JSON envelope** consumers can rely on.
5. **Configurable routing** with sensible defaults (two topics), per-user ordering via message keys.
6. Production-ready transport: TLS, SASL (PLAIN/SCRAM/OAUTHBEARER), any producer property.
7. Safe by default with PII/secrets (no credentials, no admin representations unless explicitly enabled).
8. Observable: metrics + rate-limited logs; never log payloads.

## Non-goals (for now)

- Exactly-once delivery, global ordering, consumer-side anything.
- Avro/Protobuf/Schema Registry (maybe later behind a serializer option).
- One-topic-per-event-type as the default (supported only via explicit overrides).
- Admin UI for configuration (server-level SPI config only; per-realm toggling is just enabling the listener).
- Any change to Keycloak itself.

## Delivery guarantees by mode

| Mode | Milestone | Guarantee | Mechanism |
|---|---|---|---|
| `async` (default) | M1 | at-most-once | after-commit hand-off to a bounded in-memory queue → producer |
| `outbox` (opt-in) | M3 | at-least-once (duplicates keep the same `id`) | write to Keycloak's `OUTBOX_ENTRY` in the request transaction → cluster-aware drainer → producer |

## Design

### Runtime flow (M1, `async` mode)

```
request thread                                   │ sender thread (1, daemon)          │ Kafka producer I/O thread
─────────────────────────────────────────────────┼────────────────────────────────────┼──────────────────────────
EventBuilder.success()/error()                   │                                    │
 └─ KafkaEventListenerProvider.onEvent(e)        │                                    │
     ├─ filter (type include/exclude)            │                                    │
     ├─ map → Envelope → JSON bytes  (needs      │                                    │
     │   session: realm name lookup — do it NOW) │                                    │
     └─ EventListenerTransaction.addEvent(...)   │                                    │
 ... request transaction commits ...             │                                    │
 └─ afterCompletion commitImpl()                 │                                    │
     └─ queue.offer(record)  (never blocks;      │ take() → producer.send(rec, cb)    │ batches, retries,
        full → drop + count)                     │ (may block ≤ max.block.ms here,    │ acks; cb → metrics,
                                                 │  never on the request thread)      │ rate-limited WARN
```

- One `KafkaEventListenerProvider` per `KeycloakSession`; on first event it creates one
  `EventListenerTransaction` and enlists it with `session.getTransactionManager().enlistAfterCompletion(tx)`.
  Rollback clears buffered events.
- Error events arrive in Keycloak's inner `runJobInTransaction` session → a separate provider instance →
  its own after-completion → published when that inner transaction commits. No special casing needed;
  cover it with a unit test.
- Build the envelope bytes in `onEvent` (request thread, session open). Do not touch the session in
  `commitImpl`.
- Factory owns the singletons: config, `KafkaProducer<String, byte[]>`, the bounded queue + sender thread,
  metrics. Created in `postInit`, closed in `close()` (stop accepting → drain queue for ≤ `close-timeout-ms`
  → `producer.close(Duration)`).
- Create the producer with **serializer instances** (`new StringSerializer()`, `new ByteArraySerializer()`)
  and with the thread context classloader temporarily set to the plugin's classloader — avoids the
  Quarkus/Keycloak provider classloader problems other plugins hit (SnuK87 1.1.5 "serializer classloader
  fix").

### Envelope v1 (CloudEvents 1.0, structured JSON mode)

Kafka value = UTF-8 JSON; header `content-type: application/cloudevents+json; charset=UTF-8`.

```jsonc
{
  "specversion": "1.0",
  "id": "<Keycloak event id (UUID)>",          // stable; duplicates (outbox mode) keep it
  "source": "keycloak:<realmId>",              // TODO(D2): realm id vs. issuer URL
  "type": "org.keycloak.event.LOGIN",          // admin: "org.keycloak.admin.<RESOURCE_TYPE>.<OPERATION>"
  "time": "2026-10-19T12:34:56.789Z",          // RFC 3339 from event.getTime()
  "subject": "<userId>",                       // admin: resourcePath
  "datacontenttype": "application/json",
  "kcschemaversion": 1,                        // CloudEvents extension attrs: lowercase alnum only
  "kcrealmid": "<realmId>",
  "kcrealmname": "<realm name at event time>",
  "kckind": "user",                            // "user" | "admin"
  "data": {
    // user event
    "type": "LOGIN", "clientId": "...", "userId": "...", "sessionId": "...",
    "ipAddress": "...",                        // omitted when include-ip-address=false
    "error": null,
    "details": { "auth_method": "openid-connect", "username": "..." }   // after redaction/allowlist
    // admin event (instead)
    // "operationType": "CREATE", "resourceType": "USER", "resourcePath": "users/<id>",
    // "authDetails": { "realmId": "...", "clientId": "...", "userId": "...", "ipAddress": "..." },
    // "error": null, "details": {...},
    // "representation": { ... }               // ONLY if includeRepresentation && include-representation=true
  }
}
```

Rules: never emit credentials, secrets, tokens, request bodies; redact `details` keys matching
`(?i)(password|secret|token|credential|otp|code_verifier)`; treat Keycloak's `includeRepresentation=false` as a
hard ceiling; schema changes that remove/rename fields bump `kcschemaversion` (a MAJOR release).

### Routing, keys, headers

- Topics: `topic-events` (default `keycloak.events`), `topic-admin-events` (default `keycloak.admin-events`).
  Both accept placeholders `{realmId}`, `{realmName}`; result sanitized to Kafka's legal chars
  `[a-zA-Z0-9._-]`, max 249 chars; invalid template → fail fast at startup.
- Optional `topic-overrides`: comma list `EVENT_TYPE=topic` (e.g. `LOGIN_ERROR=keycloak.security`).
- Filters: `include-event-types` / `exclude-event-types` (user `EventType` names), `include-admin-resource-types`
  / `exclude-admin-resource-types`. Needed because Keycloak's realm "enabled event types" only filters storage.
- Key (ordering scope):
  - user event: `<realmId>:<userId>`; no userId → `<realmId>:<sessionId>`; neither → `<realmId>`.
  - admin event: `<realmId>:<first two resourcePath segments>` (e.g. `users/<id>`) so all changes to one entity
    stay ordered.
- Headers: `content-type`, `kc-kind`, `kc-event-type`, `kc-realm-id`, `kc-schema-version` — lets consumers
  filter without parsing.

### Configuration (SPI scope `events-listener` / provider `kafka-events`)

| Key | Default | Notes |
|---|---|---|
| `bootstrap-servers` | — | missing → listener disabled with one WARN at startup (deploying the JAR unconfigured must be harmless) |
| `topic-events` / `topic-admin-events` | see above | templates |
| `topic-overrides` | — | |
| `include-*/exclude-*` filters | — | |
| `include-ip-address` | `true` | |
| `include-representation` | `false` | admin events |
| `allowed-detail-keys` | — | comma list of extra `details` keys to export (exact, case-insensitive) |
| `redacted-detail-keys` | — | comma list of `details` keys to always redact; wins over `allowed-detail-keys` |
| `delivery-mode` | `async` | `outbox` in M3 |
| `queue-capacity` | `10000` | async mode in-memory queue; full → drop + `dropped` metric |
| `close-timeout-ms` | `5000` | shutdown drain budget |
| `producer-properties-file` | — | `.properties` with any Kafka producer config (TLS/SASL secrets live here) |
| `producer-<kafka-key-with-dashes>` | — | passthrough, e.g. `producer-security-protocol=SASL_SSL` → `security.protocol`; `--` option form only; not for `ssl.provider` (see D3); unknown Kafka key → fail fast |

Option names must not end in `-enabled`, `-provider` or `-provider-default`: Keycloak treats such SPI keys as
build-time and refuses an optimized start when they're set at runtime (RESEARCH §6.1).

Producer defaults we set (overridable): `acks=all`, `enable.idempotence=true` (dropped when the user sets
`acks`/`retries` without it, so Kafka can fall back instead of rejecting), `linger.ms=20`,
`compression.type=lz4` (D4), `max.block.ms=1000` (sender thread only), `request.timeout.ms=10000` and
`delivery.timeout.ms=30000` (Kafka requires delivery ≥ linger + request), `client.id=keycloak-<hostname>`.
`bootstrap.servers` always comes from `bootstrap-servers`; `key.serializer`/`value.serializer` are rejected.

Invalid values (bad template, unknown mode, unparsable number) → **fail fast** at startup with a message naming
the key. Missing bootstrap servers → **disabled**, not failed.

### Metrics (M2)

Micrometer (Keycloak ships `micrometer-core`; exposed on Keycloak's metrics endpoint when metrics are enabled):
`keycloak_kafka_events_enqueued_total{kind}`, `_sent_total{kind,topic}`, `_failed_total{kind,reason}`,
`_dropped_total{kind,reason=queue_full|disabled|filtered}`, `_queue_size` gauge, outbox gauges in M3.

## Open decisions

Record the decision + reason here when made.

- **D1 — envelope shape.** CloudEvents structured mode (current proposal) vs. binary mode (`ce_*` headers,
  raw `data` as value) vs. custom. Proposal: structured; revisit only with a concrete consumer need.
- **D2 — `source` attribute.** `keycloak:<realmId>` (stable, opaque) vs. realm issuer URL (readable, changes
  with hostname). Proposal: `keycloak:<realmId>`, issuer URL as extension later if asked.
- **D3 — config passthrough.** `producer-*` passthrough via `getPropertyNames()` vs. only
  `producer-properties-file`. Verified 2026-10-19 (RESEARCH §6.1): enumeration works for env (`__` form), CLI
  and keycloak.conf; keys come back fully prefixed; dash→dot mapping is lossless for all producer keys.
  **Decided 2026-10-19: support both, precedence defaults < file < `producer-*` options.** Caveats to
  document: `--` form only (legacy single-separator keys aren't enumerated); `ssl.provider` only via the file
  (a runtime `producer-ssl-provider` makes `start --optimized` exit). Secrets (SASL/keystore passwords) are
  better in the file, since Keycloak masks `kc.spi-*` values in `show-config` but env/CLI leak elsewhere.
- **D4 — JAR size / compression codecs.** zstd-jni + snappy-java are most of the 20 MB and extract native libs
  to a temp dir at runtime (fails on read-only/noexec `/tmp` unless configured). Options: (a) keep all,
  default `compression.type=none`; (b) bundle only lz4 (pure-Java fallback) and document that zstd/snappy need
  the extra JARs in `providers/`; (c) two artifacts (slim / full, via a shade classifier). Proposal: (b).
- **D5 — relocation.** Shade without relocation (current) vs. relocate `org.apache.kafka` to avoid clashing with
  another provider that bundles kafka-clients. Relocation is risky with Kafka's class-name-by-string config;
  keep unrelocated unless a clash is reported.

## Milestones

### M0 — Scaffold ✅

- [x] Single-module Maven (shaded JAR with kafka-clients, `e2e` profile); flattened from listener/dist/e2e
      modules on 2026-10-19 — one module is all a single listener needs
- [x] Docker stack: Keycloak 26.8.1 + Kafka 4.3.1 (KRaft) + optional kafka-ui; demo realm `kafka-demo`
- [x] Smoke e2e (provider registered, realm wired, login unaffected) — passing
- [x] CI copied from keycloak-otp (build, e2e, SpotBugs, dependency-check, release)
- [x] Claude setup: CLAUDE.md, hooks (git guard, version sync, session start), skills (release, bump-keycloak, verify-events)

### M1 — Async publishing (MVP, target `0.1.0`)

- [x] Verify `Config.Scope.getPropertyNames()` behaviour under Quarkus (env + CLI keys) and how
      `getConfigMetadata()` must declare options in 26.8.1; record result under D3 and RESEARCH §6
- [x] `KafkaEventsConfig` (immutable record) parsed from `Config.Scope` with validation + unit tests
- [ ] `Envelope` model + `EnvelopeMapper` for `Event` and `AdminEvent` (redaction, representation ceiling,
      ip toggle) using `org.keycloak.util.JsonSerialization` + unit tests (golden JSON fixtures)
- [ ] `TopicRouter` (templates, sanitization, overrides) and `KeyStrategy` + unit tests
- [ ] `EventFilter` (include/exclude) + unit tests
- [ ] `KafkaSender`: bounded queue + single sender thread + producer callbacks; injectable `Producer` so unit
      tests use Kafka's `MockProducer`; never blocks `offer`
- [ ] Provider: `EventListenerTransaction` + `enlistAfterCompletion`; rollback drops; error-event inner-session
      path covered by a test
- [ ] Factory lifecycle: disabled-when-unconfigured, fail-fast on invalid config, `postInit` creates sender,
      `close()` drains with timeout; classloader-safe producer creation
- [ ] E2E: login → record on `keycloak.events` (key, headers, envelope fields); wrong password →
      `LOGIN_ERROR`; admin create user → `keycloak.admin-events`; verify `consumeTopic` output column order
      (headers/key/value) and fix `helpers.js` if needed
- [ ] E2E: `docker compose stop kafka` → login still 200 and fast (< 500 ms) → `start kafka`
- [ ] README: install, config table, envelope example, guarantees, compatibility matrix
- [ ] Resolve D1, D2, D4 (at least provisionally) before tagging `0.1.0`

### M2 — Hardening

- [ ] TLS + SASL/SCRAM e2e (compose profile `secure` with a SASL_SSL listener and generated certs)
- [ ] Micrometer metrics + e2e scrape check (`KC_METRICS_ENABLED=true`)
- [ ] Rate-limited failure logging (one WARN per N seconds with counts)
- [ ] Topic overrides + filters e2e
- [ ] Declare options in `getConfigMetadata()` for documentation (26.8.1 doesn't display it for event listeners,
      and `show-config` already lists every `kc.spi-*` key masked — RESEARCH §6.2)
- [ ] Load sanity: 1k logins/s locally with Kafka down → no latency regression, bounded memory
- [ ] SpotBugs clean, dependency-check clean (we ship kafka-clients + codecs)

### M3 — Optional at-least-once (`delivery-mode=outbox`)

- [ ] Spike: confirm `OUTBOX_ENTRY` exists without the SSF feature flag in 26.8.1; read how SSF registers its
      `OutboxDrainerTask` (TimerProvider + `ClusterAwareScheduledTaskRunner`); record in RESEARCH §4
- [ ] `KafkaOutboxDeliveryHandler` (`entryKind = "kafka-events"`): `send(...).get(timeout)` → delivered /
      retry / dead-letter on non-retriable (e.g. `RecordTooLargeException`)
- [ ] Capture in `onEvent` via `OutboxStore.enqueuePending` inside the dispatching transaction (no
      after-commit hook in this mode)
- [ ] Config: drain interval, batch size, retention, max age; metrics for queue age / dead letters
- [ ] E2E: Kafka down → logins → Kafka up → all events delivered exactly once or with duplicate ids only
- [ ] Document that this mode depends on Keycloak-internal classes and may break on Keycloak bumps

### M4 — Publish

- [ ] Create GitHub repo `arados/keycloak-kafka-event-listener` (created by the user 2026-10-19), push, enable
      Actions, add `NVD_API_KEY` secret
- [x] Artifact/JAR renamed to `keycloak-kafka-event-listener` to match the repo (provider id stays `kafka-events`)
- [ ] Release `0.1.0` via the `release` skill
