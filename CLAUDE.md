# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Start here

`docs/` holds the maintainer's **local, git-ignored** working notes — never commit them (the `.gitignore`
entry keeps them out; don't link to them from README, code or anything else that is committed). If they exist:

1. **Read `docs/PLAN.md`** — goals, design, open decisions (D1–D5) and the milestone checklist.
   The SessionStart hook tells you the next unchecked task; work top-down unless the user says otherwise.
2. **Read `docs/RESEARCH.md`** when a task touches Keycloak internals (event dispatch,
   transactions, outbox), bundled libraries, or config plumbing — it holds verified facts with sources so you
   don't have to rediscover them. Add new findings there, dated.
3. Tick tasks in PLAN.md when the PR that completes them lands. Record decisions under "Open decisions".

Anything users need (configuration, envelope, guarantees) belongs in README.md; code comments must stand on
their own.

## Project Overview

Keycloak event listener SPI plugin that publishes Keycloak **user events** and **admin events** to
**Apache Kafka**. Repo: https://github.com/arados/keycloak-kafka-event-listener. Single-module Maven project; the build output is one shaded JAR you drop into Keycloak's
`providers/`.

- **Keycloak version**: 26.8.1
- **Kafka client**: 4.4.0 (bundled); dev broker image `apache/kafka:4.3.1` (KRaft)
- **Java**: 21
- **Provider id**: `kafka-events` (Realm settings → Events → Event listeners)
- **Package**: `hr.delmisoft.keycloak.kafka`
- **Status**: async (at-most-once) and outbox (at-least-once) delivery, metrics, TLS/SASL; unit + e2e tested;
  not released yet

Sibling project `../keycloak-otp` (same author) is the template this repo was created from: same build,
CI, e2e wiring, versioning rules and test style (otp is multi-module because it ships several features; this
repo is deliberately one module). Look there for working examples of Keycloak SPI patterns,
especially `otp-login`'s after-commit async delivery (`enlistAfterCompletion` → executor).

## Project Structure

```
pom.xml     — the only Maven module; maven-shade-plugin bundles kafka-clients + lz4-java (no native libs, D4); `e2e` profile
src/        — EventListenerProvider(+Factory), config, envelope, routing, sender, metrics, outbox; unit tests
docker/     — `secure` compose profile setup (demo certs, SCRAM client config)
e2e/        — Playwright tests against the docker compose stack (plain npm project, run by the `e2e` profile)
docs/       — local only, git-ignored: PLAN.md (work plan), RESEARCH.md (verified background)
.claude/    — shared Claude Code settings, hooks, skills (see "Claude Code setup")
```

## Build & Test Commands

```bash
mvn clean package                                   # Build the plugin JAR (target/keycloak-kafka-event-listener-*.jar)
mvn test                                            # Unit tests
mvn test -Dtest=KafkaEventListenerProviderFactoryTest                       # One class
mvn test -Dtest=KafkaEventListenerProviderFactoryTest#providerIdIsStable    # One method
mvn verify -P e2e                                   # Unit tests + docker up + Playwright + docker down
mvn compile spotbugs:check                          # SpotBugs + Find Security Bugs
jar tf target/keycloak-kafka-event-listener-*.jar | less        # Inspect what the shaded JAR contains
```

## Docker Development

```bash
docker compose up --build -d          # Keycloak + Kafka (rebuilds the plugin JAR)
docker compose --profile secure up --build -d   # + production-style cluster (:8082, :8083): Postgres, outbox, SASL_SSL, zstd
docker compose --profile ui up -d     # + kafka-ui on http://localhost:8081
docker compose --profile secure down -v   # Stop and wipe everything (fresh realm import next time)
node e2e/load/load.js 1000 30         # Load sanity: ~1k events/s, Kafka up vs. stopped (restarts it after)
docker compose logs keycloak          # Keycloak logs (listener warnings show here)
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic keycloak.events --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --formatter-property print.headers=true   # prints HEADERS<TAB>KEY<TAB>VALUE
```

- Admin console: http://localhost:8080/admin/ (admin/admin); metrics on http://localhost:9000/metrics
  (`KC_METRICS_ENABLED=true`; the listener's meters are `keycloak_kafka_events_*`)
- Demo realm `kafka-demo`: user `testuser` / `password`, public client `kafka-demo-client` (direct grants on),
  events + admin events enabled, listeners `jboss-logging`, `kafka-events`
- Kafka: `kafka:9092` inside the compose network, `localhost:9094` from the host
- A one-shot `kafka-init` service creates `keycloak.events`, `keycloak.admin-events` and
  `keycloak.events.kafka-demo.logout` (3 partitions) before Keycloak starts; other topics are auto-created
- Listener config is passed as env `KC_SPI_EVENTS_LISTENER__KAFKA_EVENTS__<KEY>` (see docker-compose.yml). The
  main Keycloak routes LOGOUT to `keycloak.events.{realmName}.logout` and filters out `USER_INFO_REQUEST` and
  `REALM_ROLE` admin events (exercised by `routing.spec.js`)
- `secure` profile (production-style): `secure-secrets` (one-shot, `docker/secure/generate-secrets.sh`) writes a
  self-signed broker cert, truststore and `kafka-client.properties` into a volume; `kafka-secure` has SASL_SSL on
  `:9095` (SCRAM user created by `kafka-secure-init`) plus in-network PLAINTEXT `:9092` for the console tools;
  `postgres-secure`; `keycloak-secure` (:8082, metrics :9002) and `keycloak-secure-2` (:8083, :9003) run
  `start --optimized` from Dockerfile target `keycloak-prod` (zstd-jni in `providers/`, `--db=postgres`, health,
  metrics) as a jdbc-ping cluster, publishing over SASL_SSL with zstd and `delivery-mode=outbox`
  (`outbox-kafka-down.spec.js` stops `kafka-secure`, its own Playwright project like `kafka-down.spec.js`).
  Node 1 takes ~1–2 min to start on a fresh database
- The user also runs an unrelated `kafka` container on host ports 9092/29092 — don't touch it; this stack
  only publishes 8080, 9000, 9094 (+ 8082, 8083, 9002, 9003 with `secure`, 8081 with `ui`)

Use the **`verify-events`** skill to exercise the stack end to end.

## Key design rules

These are the invariants every change must keep. Details and rationale in PLAN.md "Design".

- **Never block or fail the request.** `onEvent` only filters, maps to bytes and buffers (async) or inserts one
  outbox row (outbox mode). Kafka I/O happens on our own threads (sender / outbox drainer), never on the request
  thread or Keycloak's shared timer thread. A full queue drops (and counts) instead of waiting. Kafka
  down/slow/misconfigured must not add measurable latency to logins.
- **Publish only committed events** (async mode): buffer the records built in `onEvent` in our own
  after-completion transaction (`PublishAfterCommitTransaction`, not Keycloak's `EventListenerTransaction`, which
  maps at commit time), enlisted via `session.getTransactionManager().enlistAfterCompletion(...)`; commit only
  offers them to the sender's queue, rollback discards. Its `commitImpl` must never throw (Keycloak rethrows it
  out of the request's commit — RESEARCH §3.4).
- **Do session work inside `onEvent`.** Anything needing `KeycloakSession` / `RealmModel` (issuer URL for
  `source`, etc.) happens in `onEvent` — usually the request thread, but brute-force lockout events arrive on
  Keycloak's `bruteforce` executor; the after-commit callback and sender thread must not touch the session.
- **Envelope is a public contract.** CloudEvents-shaped JSON with `kcschemaversion`. Removing/renaming fields
  = new schema version + MAJOR release. Golden-fixture tests guard it.
- **No secrets, ever.** Never export credentials, tokens, secrets, request bodies; redact sensitive `details`
  keys; admin `representation` only when Keycloak passes `includeRepresentation=true` **and**
  `include-representation=true` is configured. Never log payloads.
- **Unconfigured = disabled, misconfigured = fail fast.** Missing `bootstrap-servers` → single WARN, no-op.
  Invalid values → startup error naming the key.
- **Bundle only what Keycloak lacks**: kafka-clients + lz4-java (classes only; its JNI libs are filtered out).
  zstd-jni/snappy-java are deliberately not bundled (D4) — users add them to `providers/`. Keycloak provides
  slf4j, jackson, micrometer — never shade those. Re-check on Keycloak bumps (`bump-keycloak` skill).
- **Producer creation is classloader-safe**: pass serializer instances; set the TCCL to the plugin classloader
  while constructing the producer.
- **Outbox mode** (`outbox/`): `OutboxWriter` inserts in the dispatching transaction (own transaction if the
  session's is completed); `KafkaOutboxDrainer` (a `ScheduledTask`, scheduled after `PostMigrationEvent` via
  `ClusterAwareScheduledTaskRunner`, run on our `kafka-events-outbox` thread) sends and classifies: unreachable /
  retriable → retry without counting an attempt; too large / invalid topic → dead letter; other → backoff +
  `outbox-max-attempts`. Its simple class name is Keycloak's cluster lock key — keep it unique.
- Keycloak internals (`model/jpa` outbox, `model/storage-private` cluster runner, `server-spi-private`) are
  allowed but must be isolated behind our own interfaces and called out in docs, because they can change in any
  Keycloak minor. Only `outbox.KeycloakOutboxRepository` and `outbox.KafkaOutbox` touch the outbox internals.

## Testing

**Unit tests** (`src/test/`): JUnit 6 + Mockito + Hamcrest, mocking Keycloak's
`KeycloakSession` / `RealmModel` / `KeycloakTransactionManager` (same style as keycloak-otp). For Kafka use
`org.apache.kafka.clients.producer.MockProducer` — keep the producer injectable (package-private constructor or
supplier) so no test needs a broker. Envelope tests compare against golden JSON fixtures in
`src/test/resources/`.

**E2E tests** (`e2e/tests/*.spec.js`): Playwright test runner (mostly its `request` API; Chromium installed for
browser-flow tests). `helpers.js` provides `adminToken`, `passwordLogin`, `demoRealm` (id + issuer),
`decodeJwt`, `consumeTopic(topic)` / `waitForRecord(topic, predicate)`, which run the broker's console consumer
inside the `kafka` container — no Node Kafka client dependency — `stopKafka`/`startKafka`, `topicCodecs` (batch
compression via `kafka-dump-log.sh`) and `metricValue` (scrapes `:9000/metrics`). Each takes an optional stack,
`MAIN` (default) or `SECURE`. The suite needs the `secure` profile up. Match records by
something unique to the test (session id, new user id, unseen event id): topics keep earlier runs' records.
`kafka-down.spec.js` stops the broker, so it is its own Playwright project that depends on the main one (runs
last).
`mvn verify -P e2e` manages the docker lifecycle exactly like keycloak-otp, always with `--profile secure`
(antrun: down → up --build → wait for both Keycloaks → npm ci → playwright → down; exit code recorded in
`target/e2e-exit-code.txt`).

## CI Pipeline

`.github/workflows/ci.yml` (copied from keycloak-otp):
- **build**: `mvn clean verify`, uploads the shaded JAR
- **e2e**: `mvn verify -P e2e -DskipTests`, uploads Playwright report and dumps Keycloak + Kafka logs on failure
- **spotbugs**: SpotBugs + Find Security Bugs on every push/PR
- **dependency-check**: OWASP NVD scan on main/tags only (needs `NVD_API_KEY` secret), fails on CVSS ≥ 9.
  This matters more here than in keycloak-otp: we **ship** kafka-clients and lz4-java.
- **release**: on `v*` tag, builds and publishes the JAR as a GitHub Release

Exclusions: `spotbugs-exclude.xml` (narrow, commented); `dependency-check-suppressions.xml` (only the
Keycloak CPE name-collision false positive — never suppress kafka-clients/lz4-java findings without analysis).

## Versioning & Releases

Versions are `MAJOR.MINOR.PATCH`, but the parts are **not** used the SemVer way. Bump by what the release contains:

- **MAJOR** — new features (new delivery mode, routing option, envelope field, config option, or other new capability)
- **MINOR** — Keycloak library update (`keycloak.version` / base image bump)
- **PATCH** — bug fixes and security fixes, including dependency CVE bumps and new settings added only to close a security issue

If a release contains several kinds of change, the highest part wins; lower parts reset to 0. Before 1.0.0,
features bump MINOR (0.1.0 → 0.2.0); ask the user before cutting 1.0.0.

Use the **`release`** skill. Summary: on a branch run
`mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`, commit as
`Release X.Y.Z`, PR to `main`; after merge wait for the `main` CI run (only one with dependency-check); then
`git tag -a vX.Y.Z -m "Release X.Y.Z"` on the merge commit and push the tag.

## Git workflow

- Never commit on `main`; branch first (`feat/…`, `fix/…`, `chore/…`, `docs/…`). Enforced by a hook.
- One PR per logical change, base `main`. **No stacked PRs**: CI only runs for PRs targeting `main`, and
  merging a PR whose base is another branch lands it there, not on `main`.
- Never force-push; never push to `main` directly. Enforced by a hook.
- Ask the user before `git push`, `gh pr create`, `gh pr merge`, tagging, or creating the GitHub repo
  (these are configured as "ask" in `.claude/settings.json`).
- Remote: `origin` = https://github.com/arados/keycloak-kafka-event-listener (public). The local folder is still
  named `keycloak-kafka-events`, so docker compose container names keep that prefix.

## Claude Code setup (`.claude/`, committed)

- `settings.json` — allow-list for routine build/test/docker/read-only git & gh commands; "ask" for
  push/tag/PR/release; registers the hooks below.
- Hooks (`.claude/hooks/`):
  - `session-start.sh` (SessionStart) — injects branch, dirty-file count, last commit and the next unchecked
    PLAN.md task.
  - `guard-git.sh` (PreToolUse/Bash) — denies `git commit` on main, pushes to main, and force-pushes.
  - `check-version-sync.sh` (PostToolUse/Edit|Write on `pom.xml`/`Dockerfile`) — blocks with a reason if
    `keycloak.version` ≠ Dockerfile base tag.
- Skills (`.claude/skills/`): `release`, `bump-keycloak`, `verify-events`.
- Personal overrides go in `.claude/settings.local.json` (git-ignored).
