#!/usr/bin/env bash
# PostToolUse(Edit|Write) consistency check. Runs only when pom.xml or the Dockerfile was touched,
# and verifies <keycloak.version> in pom.xml == the quay.io/keycloak/keycloak tag in Dockerfile.
# On mismatch it returns decision "block" so the reason is fed back to Claude to fix.
set -euo pipefail

file=$(jq -r '.tool_input.file_path // .tool_response.filePath // empty')
case "$file" in
  */pom.xml|*/Dockerfile) ;;
  *) exit 0 ;;
esac

root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
[[ -f "$root/pom.xml" && -f "$root/Dockerfile" ]] || exit 0

pom_kc=$(sed -n 's#.*<keycloak.version>\(.*\)</keycloak.version>.*#\1#p' "$root/pom.xml" | head -1)
docker_kc=$(sed -n 's#^FROM quay.io/keycloak/keycloak:\([^ ]*\).*#\1#p' "$root/Dockerfile" | head -1)
if [[ -n "$pom_kc" && -n "$docker_kc" && "$pom_kc" != "$docker_kc" ]]; then
  reason="Version drift detected by .claude/hooks/check-version-sync.sh:"
  reason+=$'\n- '"keycloak.version in pom.xml is $pom_kc but Dockerfile uses quay.io/keycloak/keycloak:$docker_kc"
  jq -n --arg r "$reason" '{decision: "block", reason: $r}'
fi
exit 0
