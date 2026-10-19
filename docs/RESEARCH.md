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

### 3.1 Event model getters (verified 2026-10-19 with `javap`, Keycloak 26.8.1)

`Event`, `AdminEvent`, `AuthDetails`, `EventType`, `OperationType`, `ResourceType` live in
**`keycloak-server-spi-private`** (not `keycloak-server-spi`). `JsonSerialization` is in `keycloak-core`.

- `Event`: `getId`, `getTime` (epoch ms), `getType` (`EventType`), `getRealmId`, **`getRealmName`**, `getClientId`,
  `getUserId`, `getSessionId`, `getIpAddress`, `getError`, `getDetails` (`Map<String,String>`, may be null).
  `EventBuilder` calls `setRealmName`, so the realm name is on the event without a session lookup.
- `AdminEvent`: `getId` (the event id; `getResourceId()` is derived from the last `resourcePath` segment),
  `getTime`, `getRealmId`, `getRealmName`, `getAuthDetails`, `getOperationType`, `getResourcePath`,
  `getRepresentation` (JSON **string**), `getError`, **`getDetails`** (`Map<String,String>`), `getResourceType`
  (enum) and `getResourceTypeAsString` — the field is stored as a string (custom resource types), so use the
  string getter.
- `AuthDetails`: `getRealmId`, `getRealmName`, `getClientId`, `getUserId`, `getIpAddress`.
- `AdminEventBuilder.representation(Object)` runs `StripSecretsUtils.stripSecrets(session, rep)` before
  `JsonSerialization.writeValueAsString`, so Keycloak already masks secrets in admin representations.
- `JsonSerialization.mapper` (Jackson 2.21.7): `Jdk8Module` + `JavaTimeModule`, `FAIL_ON_EMPTY_BEANS=false`,
  default property inclusion `NON_NULL`. Records serialize fine.

### 3.2 Resolving the realm issuer for `source` (D2, verified 2026-10-19 from 26.8.1 source)

- CloudEvents 1.0: `source` is a required URI-reference (absolute URI recommended); only `source`+`id` must
  be unique. <https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md>
- Issuer = `Urls.realmIssuer(baseUri, realmName)` (`services/.../services/Urls.java:166`, percent-encodes the
  name) — what `TokenManager` and `OIDCWellKnownProvider` use. Base: realm attribute `frontendUrl` if set,
  else `session.getContext().getUri(UrlType.FRONTEND).getBaseUri()` (`HostnameV2Provider`, cached per context).
- Realm `frontendUrl` is read from the **context** realm by `HostnameV2Provider.getRealmFrontUriBuilder`, so
  read the attribute from the **event** realm ourselves when the two differ.
- Varies per request when `hostname-strict=false` (dev mode / Host header) or `hostname` is host-only
  (scheme/port from the request). Stable when `hostname` is a full URL or the realm sets `frontendUrl`.
- Dispatch paths:
  - Error events: inner session from `runJobInTransactionWithResult` gets realm/client cloned
    (`KeycloakModelUtils.java:418-545`) and `QuarkusKeycloakContext` resolves the HTTP request from
    `CurrentRequestManager` on the same thread → `getUri()` works (code reading; confirm in e2e).
  - Brute-force lockout events (`USER_DISABLED_BY_*_LOCKOUT`) run on the `bruteforce` executor thread with a
    synthetic `BruteForceHttpRequest` (`DefaultBruteForceProtector.java:251-266`) → `getUri()` works. Hence the
    design rule is "inside `onEvent`", not "on the request thread".
  - Admin events: context realm = target realm (`RealmsAdminResource.java:230`) = `AdminEvent.realmId`,
    **except** realm CREATE/DELETE which carry the admin's realm (`RealmsAdminResource.java:182-183,583-585`).
    `authDetails.realmId` is the admin's realm — not for `source`.
  - No HTTP context (scheduled jobs): `getUri()` throws `ContextNotActiveException` unless `frontendUrl` or a
    full-URL `hostname` is set → catch and use `urn:keycloak:realm:<realmId>`.
- Precedent: Keycloak SSF transmitter uses the realm issuer for SET `iss`
  (`ssf/transmitter/.../support/SsfUtil.java:27-57`, chain: realm `frontendUrl` → `KC_HOSTNAME_URL` → full-URL
  `hostname` → `ctx.getUri()`). Keycloak discussion #25133 proposes the realm admin URL (no maintainer reply).
  No existing Keycloak→Kafka listener sets a CloudEvents `source`.

### 3.3 Event `details` keys (verified 2026-10-19 from 26.8.1 source)

- `org.keycloak.events.Details` (server-spi-private) is an **interface** with 103 `String` constants (all
  `public static final`, distinct values). A few are values or prefixes, not keys: `ACTOR_TYPE_USER` ("user"),
  `ACTOR_TYPE_CLIENT`, `CONSENT_VALUE_*`, `PREF_PREVIOUS` ("previous_"), `PREF_UPDATED` ("updated_").
- **Over-redaction finding**: our first redaction rule, substring regex
  `(?i)(password|secret|token|credential|otp|code_verifier)`, matched 24 of those 103 values, none of which holds
  a secret: `access_token_expiration_time`, `age_of_refresh_token`, `credential_id`, `credential_type`,
  `credential_user_label`, `logout_triggered_by_action_token`, `refresh_token_id`, `refresh_token_provider_id`,
  `refresh_token_sub`, `refresh_token_type`, `requested_token_type`, `selected_credential_id`,
  `subject_token_client_id`, `token_exchange_revoked_clients`, `token_id`, `token_issued_for`, `token_type`,
  `updated_refresh_token_id`, `validate_access_token`, `verifiable_credential_format`,
  `verifiable_credential_pre_authorized`, `verifiable_credential_target_client_id`,
  `verifiable_credential_target_user_id`, `verifiable_credentials_issued`. These are what consumers correlate
  on (token/session ids, credential type). Conversely a substring rule misses secrets stored under innocent names
  by extensions. Hence the allowlist-based policy in PLAN "Envelope v1" (`envelope.DetailRedaction`).
- Keycloak core never puts a password, client secret, token value or code verifier into `details` (no
  `Details` constant names one); token-related details are ids and types.
- Keys set by core **outside** `Details` (found with `git grep -nE '(^|[^A-Za-z])detail\('` over
  `*/src/main/java` of the 26.8.1 tree, plus `getDetails().put`; paths relative to the Keycloak repo):
  - `authenticator` — `services/.../authenticators/broker/IdpDetectExistingBrokerUserAuthenticator.java:67`
  - `validation_method` — `services/.../broker/oidc/AbstractOAuth2IdentityProvider.java:923`,
    `OIDCIdentityProvider.java:1027`, `services/.../social/bitbucket/BitbucketIdentityProvider.java:77`
  - `detail` — `services/.../protocol/oidc/endpoints/AuthorizationEndpointChecker.java:481`,
    `TokenIntrospectionEndpoint.java:212`, `services/.../protocol/oidc/par/endpoints/AbstractParEndpoint.java:97`
  - `device_code_user_session_id` — `services/.../grants/device/endpoints/DeviceEndpoint.java:259`
  - `existing_email`, `existing_username` — `"existing_" + attr` in
    `services/.../authenticators/broker/IdpCreateUserIfUniqueAuthenticator.java:146` (attr is
    `UserModel.EMAIL`/`USERNAME`, lines 167–190)
  - `previous_username`, `updated_username` — `IdpReviewProfileAuthenticator.java:216-217` and the generic branch
    of `services/.../userprofile/EventAuditingAttributeChangeListener.java:59`
  - `previous_locale`, `updated_locale` — `services/.../locale/DefaultLocaleUpdaterProvider.java:49-50`
  - `previous_identity_provider_identity` — `services/.../services/resources/IdentityBrokerService.java:994`
  - `WebAuthnConstants` (`services/src/main/java/org/keycloak/WebAuthnConstants.java:67-83`, used by
    `WebAuthnAuthenticator` and `WebAuthnRegister`): `public_key_credential_id`, `public_key_credential_label`,
    `public_key_credential_aaguid`, `web_authn_authenticator_user_verification_checked`,
    `web_authn_authenticated_user_id`, `first_authenticated_user_id`, `web_authn_authentication_error`,
    `web_authn_authentication_error_detail`, `web_authn_registration_error`, `web_authn_registration_error_detail`
  - `membershipType` — `MembershipType.NAME` (`core/.../representations/idm/MembershipType.java:31`) in
    `services/.../organization/admin/resource/OrganizationMemberResource.java:275`
  - `subject_issuer` — `OAuth2Constants.SUBJECT_ISSUER` in
    `services/.../protocol/oidc/tokenexchange/AbstractTokenExchangeProvider.java:143`
  - `SAML_LOGOUT_REQUEST_ID` — `services/.../protocol/saml/SamlProtocol.java:805`
  - `apiVersion` (value `v2`) — `rest/admin-v2/services/.../events/admin/v2/AdminEventV2Builder.java:43`, on
    every admin event from the v2 admin API
  - `username`, `email` via `UserModel.USERNAME`/`EMAIL` — same values as `Details` constants.
- Dynamic keys: `EventAuditingAttributeChangeListener` emits `previous_<attr>` / `updated_<attr>` for **any**
  changed user-profile attribute (first/last name and email use the `Details` constants), so realms with custom
  attributes produce realm-specific keys. They are not standard keys; operators allowlist them.
- `ssf/transmitter/.../SecurityEventTokenMapper.java:1187,1207` puts `admin=true` on synthetic `Event`s it maps to
  SSF tokens; those are not dispatched to event listeners, so `admin` is not in the list.

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
| `kafka-clients` 4.4.0 | **no** | bundle (shade). Itself embeds relocated `commons-net` 3.13.0 (`org/apache/kafka/shaded/…`) |
| `lz4-java` (`at.yawk.lz4:lz4-java` 1.11.2, the fork kafka-clients 4.4.0 depends on) | **no** | bundle the classes, **drop its JNI libraries** (§5.1) |
| `zstd-jni` 1.5.7-17, `snappy-java` 1.1.10.7 | **no** | **not bundled** (D4); excluded from the kafka-clients dependency. Users who want those codecs add the JARs to `providers/` (README "Compression") |
| `slf4j-api` | yes, 2.0.18 (+ `slf4j-jboss-logmanager`) | do **not** bundle (kafka-clients wants 1.7.36; 2.x API is compatible for its usage) |
| `jackson-databind` | yes, 2.21.7 | use Keycloak's (`org.keycloak.util.JsonSerialization`), don't bundle |
| `micrometer-core` | yes, 1.17.1 | use for metrics (M2), don't bundle |

Re-check on every Keycloak bump (`bump-keycloak` skill).

Shaded JAR size: 19.5 MB at scaffold time (kafka-clients + zstd-jni 7.2 MB + snappy-java 1.9 MB + lz4-java
0.9 MB); **11.1 MB** after D4 (2026-10-19), almost all of it kafka-clients' own classes. No `.so`/`.dylib`/`.dll`
left (`jar tf target/*.jar | grep -E '\.(so|dylib|dll)$'` is empty).

### 5.1 Compression codecs (verified 2026-10-19, kafka-clients 4.4.0, lz4-java 1.11.2)

Sources: `kafka-clients-4.4.0-sources.jar` and `lz4-java-1.11.2-sources.jar` from Maven Central; empirical
checks in `CompressionCodecsTest`, a throwaway probe (JDK 25, macOS aarch64) and the docker stack (Keycloak
image runs OpenJDK 21.0.12).

- **zstd/snappy classes load only when that codec is configured.** `Compression.of(CompressionType)` and
  `KafkaProducer.configureCompression` only instantiate `ZstdCompression`/`SnappyCompression` for
  `compression.type=zstd|snappy`; `CompressionType` itself never touches the codec libraries (it hard-codes
  zstd's level range precisely to avoid calling `Zstd.minCompressionLevel()`). A real `KafkaProducer` with
  `compression.type=lz4` constructs fine with zstd-jni and snappy-java absent.
- **Missing codec JAR fails at producer construction, not at first send.** `new KafkaProducer(...)` with
  `compression.type=zstd` (or `snappy`) and no codec JAR throws `KafkaException("Failed to construct kafka
  producer")` caused by `NoClassDefFoundError: com/github/luben/zstd/ZstdOutputStreamNoFinalizer` (snappy:
  `org/xerial/snappy/SnappyOutputStream`) from `ZstdCompression$Builder.build()` — bytecode verification of
  `ZstdCompression` loads the stream class it passes as an `OutputStream`. So it surfaces in our factory's
  `postInit`, but the message doesn't say which JAR to add; the config layer should check up front (PLAN, factory
  task). (Were verification disabled, it would instead fail at the first batch, on the sender thread.)
- **Kafka's lz4 path**: `Lz4BlockOutputStream`/`Lz4BlockInputStream` call `LZ4Factory.fastestInstance()` and
  `XXHashFactory.fastestInstance()`.
- **`fastestInstance()` tries JNI only if lz4-java was loaded by the system classloader** (or JNI is already
  loaded): `if (Native.isLoaded() || Native.class.getClassLoader() == ClassLoader.getSystemClassLoader())`,
  otherwise it goes straight to `fastestJavaInstance()`. Keycloak loads providers through Quarkus' runner
  classloader, not the system one (`jcmd 1 VM.classloaders show-classes` in the 26.8.1 container lists our
  factory under `io.quarkus.bootstrap.runner.RunnerClassLoader`, a child of `app`; the system classpath is only
  `lib/quarkus-run.jar`), so **the bundled natives were never used under Keycloak**. Probe: lz4-java in
  a non-system `URLClassLoader` → `LZ4Factory:JavaSafe | XXHashFactory:JavaUnsafe`, nothing written to
  `java.io.tmpdir`.
- **When JNI is attempted** (`Native.load()`): `System.loadLibrary("lz4-java")` from `java.library.path`, then
  extract `net/jpountz/util/<os>/<arch>/liblz4-java.<ext>` to `java.io.tmpdir` via `File.createTempFile` and
  `System.load` it. Missing resource → `UnsupportedOperationException`; unwritable tmpdir → `IOException` →
  `ExceptionInInitializerError`; noexec tmpdir → `UnsatisfiedLinkError`. `fastestInstance()` catches
  `Throwable` and falls back to Java in every case. Probe on the system classloader: JAR without natives →
  `JavaSafe`, first call 35 ms (vs 233 ms extracting JNI); read-only tmpdir with natives → `JavaSafe`. Cost of
  the fallback: each later `fastestInstance()` call retries JNI (~20 µs vs ~5 µs) — Kafka calls it once per
  compressed batch, and under Keycloak this branch isn't taken anyway.
- **Java path performance**: in 1.11.2 `LZ4Factory.unsafeInstance()` deliberately returns `safeInstance()`
  (security), and the `nativeInsecureInstance()` javadoc says the safe decompressor is faster than even the JNI
  fast decompressor. Compression on the Java path
  is slower than JNI, but our payloads are small JSON batches.
- **Decision**: exclude `net/jpountz/util/*/*/liblz4-java.*` from the shaded JAR (pom shade filter). Nothing is
  ever extracted, no native code ships, and behaviour under Keycloak is unchanged. Watch item: lz4-java's
  `UnsafeUtils` (used by `XXHashFactory:JavaUnsafe`) calls `sun.misc.Unsafe` memory-access methods that JDK 24+
  warns about and a future JDK removes; re-check on JDK/Kafka bumps.

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
