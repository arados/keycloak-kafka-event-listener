---
name: release
description: Cut a release of keycloak-kafka-events — pick the version per the project's non-SemVer rules, open the "Release X.Y.Z" PR, wait for main CI, then tag. Use when the user asks to release, cut a version, bump the version, or tag.
---

# Release keycloak-kafka-events

Versions are `MAJOR.MINOR.PATCH` but **not SemVer**. Pick the bump from what landed since the last tag
(`git log $(git describe --tags --abbrev=0)..origin/main --oneline`):

| Part  | Bump when the release contains                                                                 |
|-------|-------------------------------------------------------------------------------------------------|
| MAJOR | a new feature: new destination/routing mode, envelope field, config option, outbox, metrics…    |
| MINOR | a Keycloak library update (`keycloak.version` + Dockerfile base image)                          |
| PATCH | bug or security fixes only, incl. settings added only to close a security issue; kafka-clients CVE bumps |

Highest part wins; lower parts reset to 0 (1.3.1 + feature → 2.0.0; 1.3.1 + Keycloak bump → 1.4.0).
While still pre-1.0 (`0.x`), every feature bumps MINOR instead; ask the user before cutting `1.0.0`.

State the chosen version and the one-line reason to the user **before** doing anything.

## Steps

1. Start from fresh main: `git switch main && git pull --ff-only`.
2. Branch: `git checkout -b release-X.Y.Z`.
3. Set the version in every module (the e2e module is only in the `e2e` profile, so the profile is required):
   ```bash
   mvn -P e2e versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false -DprocessAllModules=true
   ```
   The `check-version-sync` hook will flag any module left behind.
4. Verify: `mvn -B clean verify` (unit tests + shaded JAR). If Docker is available also run `mvn -B verify -P e2e -DskipTests`.
5. Commit exactly as `Release X.Y.Z`, push the branch, open a PR to `main` (ask the user before pushing / creating the PR).
6. After the user merges it: wait for the **main** CI run to pass (`gh run list --branch main --limit 3`,
   `gh run watch <id>`). Only the main run includes OWASP dependency-check, which matters here because we
   ship kafka-clients and native compression libs.
7. Tag the merge commit and push the tag (ask first):
   ```bash
   git switch main && git pull --ff-only
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   git push origin vX.Y.Z
   ```
   The `release` CI job builds `dist/target/keycloak-kafka-events-X.Y.Z.jar` and publishes the GitHub Release.
8. Bump main back to the next snapshot only if the user wants snapshots (default: no).

Never stack the release PR on another feature branch: CI only runs for PRs whose base is `main`.
