#!/usr/bin/env bash
# PostToolUse(Edit|Write) consistency check. Runs only when a pom.xml or the Dockerfile
# was touched, and verifies:
#   1. <keycloak.version> in the root pom == the quay.io/keycloak/keycloak tag in Dockerfile
#   2. every module pom's <parent><version> == the root pom's <version>
# On mismatch it returns decision "block" so the reason is fed back to Claude to fix.
set -euo pipefail

file=$(jq -r '.tool_input.file_path // .tool_response.filePath // empty')
case "$file" in
  */pom.xml|*/Dockerfile) ;;
  *) exit 0 ;;
esac

root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
[[ -f "$root/pom.xml" && -f "$root/Dockerfile" ]] || exit 0

problems=()

pom_kc=$(sed -n 's#.*<keycloak.version>\(.*\)</keycloak.version>.*#\1#p' "$root/pom.xml" | head -1)
docker_kc=$(sed -n 's#^FROM quay.io/keycloak/keycloak:\([^ ]*\).*#\1#p' "$root/Dockerfile" | head -1)
if [[ -n "$pom_kc" && -n "$docker_kc" && "$pom_kc" != "$docker_kc" ]]; then
  problems+=("keycloak.version in pom.xml is $pom_kc but Dockerfile uses quay.io/keycloak/keycloak:$docker_kc")
fi

# Root project version: first <version> that is a direct child of <project> (i.e. after </parent>-less header).
root_ver=$(awk '/<parent>/{p=1} /<\/parent>/{p=0; next} !p && /<version>/{gsub(/.*<version>|<\/version>.*/,""); print; exit}' "$root/pom.xml")
for m in "$root"/*/pom.xml; do
  [[ -f "$m" ]] || continue
  parent_ver=$(awk '/<parent>/{p=1} p && /<version>/{gsub(/.*<version>|<\/version>.*/,""); print; exit}' "$m")
  if [[ -n "$parent_ver" && "$parent_ver" != "$root_ver" ]]; then
    problems+=("${m#$root/} declares parent version $parent_ver but root pom.xml is $root_ver (use: mvn -P e2e versions:set -DnewVersion=X -DgenerateBackupPoms=false -DprocessAllModules=true)")
  fi
done

if (( ${#problems[@]} > 0 )); then
  reason="Version drift detected by .claude/hooks/check-version-sync.sh:"
  for p in "${problems[@]}"; do reason+=$'\n- '"$p"; done
  jq -n --arg r "$reason" '{decision: "block", reason: $r}'
fi
exit 0
