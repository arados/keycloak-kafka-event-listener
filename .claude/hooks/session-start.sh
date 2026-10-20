#!/usr/bin/env bash
# SessionStart: give Claude a short orientation — branch, working-tree state, and the next
# unchecked task from docs/PLAN.md (local, git-ignored notes; skipped when absent) — so a fresh session resumes
# where the last one stopped.
set -euo pipefail

root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
branch=$(git -C "$root" branch --show-current 2>/dev/null || echo "?")
dirty=$(git -C "$root" status --porcelain 2>/dev/null | wc -l | tr -d ' ')
last=$(git -C "$root" log -1 --format='%h %s' 2>/dev/null || echo "none")
# First unchecked task, joined with its indented continuation lines.
next=$(awk '
  found && /^[[:space:]]+[^-[:space:]]/ { sub(/^[[:space:]]+/, ""); line = line " " $0; next }
  found { exit }
  /^[[:space:]]*- \[ \] / { sub(/^[[:space:]]*- \[ \] /, ""); line = $0; found = 1 }
  END { if (found) print line }
' "$root/docs/PLAN.md" 2>/dev/null || true)

ctx="Repo state: branch=$branch, uncommitted files=$dirty, last commit: $last."
if [[ -f "$root/docs/PLAN.md" ]]; then
  if [[ -n "$next" ]]; then
    ctx+=" Next unchecked task in docs/PLAN.md: $next."
  else
    ctx+=" docs/PLAN.md has no unchecked tasks."
  fi
  ctx+=" Read docs/PLAN.md before starting implementation work; tick tasks off there as they land."
  ctx+=" docs/ is local and git-ignored: never commit it."
fi

jq -n --arg c "$ctx" '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: $c}}'
