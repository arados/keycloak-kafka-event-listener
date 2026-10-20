---
name: bump-keycloak
description: Upgrade the Keycloak version this plugin builds and runs against (pom keycloak.version + Dockerfile base image), check for SPI/API breakage and changed bundled libraries, and verify end to end. Use when the user asks to update/bump/upgrade Keycloak or when a new Keycloak release is out.
---

# Bump Keycloak

A Keycloak bump is a **MINOR** release per CLAUDE.md versioning rules.

1. Find the target: `gh release list --repo keycloak/keycloak --limit 5` (skip `nightly`). Read the release
   notes / upgrading guide for anything touching events, `EventListenerProvider`, `EventBuilder`,
   `AdminEvent`, transactions (`KeycloakTransactionManager`), or `org.keycloak.events.outbox`.
2. Branch: `git checkout -b chore/keycloak-X.Y.Z`.
3. Update **both** places (the `check-version-sync` hook blocks if they diverge):
   - `pom.xml` → `<keycloak.version>`
   - `Dockerfile` → `FROM quay.io/keycloak/keycloak:X.Y.Z`
   Also update the version in `CLAUDE.md` ("Keycloak version").
4. Re-check which libraries the new Keycloak image already ships, because the shade plugin in `pom.xml` bundles only
   what Keycloak lacks:
   ```bash
   docker run --rm --entrypoint sh quay.io/keycloak/keycloak:X.Y.Z -c \
     'ls /opt/keycloak/lib/lib/main | grep -iE "kafka|slf4j|zstd|lz4|snappy|jackson-databind|micrometer-core"'
   ```
   If Keycloak now ships one of the bundled libs (or dropped slf4j/jackson/micrometer), adjust
   the shade `<artifactSet>` in `pom.xml` and record the finding in docs/RESEARCH.md §5 (local notes, git-ignored).
5. Outbox mode uses Keycloak internals (`org.keycloak.events.outbox.OutboxStore`, `OutboxEntryEntity` and its
   `OUTBOX_ENTRY` changelog in `model/jpa`, `ClusterAwareScheduledTaskRunner` in `model/storage-private`,
   `PostMigrationEvent`, `TimerProvider`). Diff them between the old and new tag:
   `gh api repos/keycloak/keycloak/compare/OLD...NEW --jq '.files[].filename' | grep -E 'events/outbox|OutboxEntry|ClusterAwareScheduledTaskRunner|jpa-changelog|EventBuilder|EventListener'`
   and adapt `outbox.KeycloakOutboxRepository` / `outbox.KafkaOutbox`; `outbox-kafka-down.spec.js` must pass.
   Also re-check event `details` keys Keycloak sets outside `org.keycloak.events.Details` (the `Details`
   constants are read by reflection; the rest are hard-coded in `DetailRedaction.EXTRA_STANDARD_KEYS`): in a
   checkout of the new tag run `git grep -nE '(^|[^A-Za-z])detail\(' -- '*/src/main/java/*.java'` and compare
   against `DetailRedaction.EXTRA_STANDARD_KEYS`. New literal keys stay redacted until added there.
6. Verify: `mvn -B clean verify`, then `mvn -B verify -P e2e`.
7. Commit `Bump Keycloak to X.Y.Z`, open PR to main. Release via the `release` skill if the user wants it shipped.
