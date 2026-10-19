#!/usr/bin/env bash
# PreToolUse(Bash) guard for the git workflow in CLAUDE.md:
#   - never commit directly on main (branch first; CI only runs for PRs into main)
#   - never push to main directly (main changes only via merged PRs)
#   - never force-push
# Emits a PreToolUse "deny" decision with the reason; exits 0 otherwise.
set -euo pipefail

input=$(cat)
cmd=$(jq -r '.tool_input.command // empty' <<<"$input")
[[ -z "$cmd" ]] && exit 0

deny() {
  jq -n --arg r "$1" '{hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: $r}}'
  exit 0
}

# Resolve the branch where the command runs, not where the project was opened: worktrees
# (.claude/worktrees/*) have their own branch. Order: `git -C <dir>`, the hook's cwd, the project dir.
git_dir=$(jq -r '.cwd // empty' <<<"$input")
if [[ "$cmd" =~ git[[:space:]]+-C[[:space:]]+([^[:space:]\;\&\|]+) ]]; then
  git_dir="${BASH_REMATCH[1]}"
fi
git_dir="${git_dir:-${CLAUDE_PROJECT_DIR:-$(pwd)}}"
branch=$(git -C "$git_dir" branch --show-current 2>/dev/null || true)

if [[ "$cmd" =~ (^|[[:space:]\;\&\|])git([[:space:]]+-C[[:space:]]+[^[:space:]]+)?[[:space:]]+commit([[:space:]]|$) ]] && [[ "$branch" == "main" || "$branch" == "master" ]]; then
  deny "Blocked by .claude/hooks/guard-git.sh: you are on '$branch'. Create a branch first (git checkout -b <type>/<topic>); main only changes through merged PRs."
fi

if [[ "$cmd" =~ (^|[[:space:]\;\&\|])git[[:space:]]+push([[:space:]]|$) ]]; then
  if [[ "$cmd" =~ [[:space:]](--force|--force-with-lease|-f)([[:space:]=]|$) ]]; then
    deny "Blocked by .claude/hooks/guard-git.sh: force-push is not allowed. If history really must be rewritten, ask the user to run it themselves."
  fi
  if [[ "$cmd" =~ [[:space:]](main|master|HEAD:main|HEAD:master)([[:space:]]|$) ]]; then
    deny "Blocked by .claude/hooks/guard-git.sh: direct push to main. Push a branch and open a PR (gh pr create --base main)."
  fi
fi

exit 0
