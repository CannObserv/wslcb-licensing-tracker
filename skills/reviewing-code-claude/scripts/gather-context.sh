#!/usr/bin/env bash
# gather-context.sh
# Prints repo state and runs the test suite for use during code review.
# Detects the git project root automatically; safe to invoke from any directory.
#
# Usage: bash skills/reviewing-code-claude/scripts/gather-context.sh [--help]
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  echo "Usage: bash skills/reviewing-code-claude/scripts/gather-context.sh"
  echo ""
  echo "Prints git status, diffs, recent commits, changed files, and pytest results."
  echo "Automatically resolves the git project root regardless of invocation directory."
  echo ""
  echo "Exit codes:"
  echo "  0  Always exits 0 (test failures are captured as output, not errors)"
  exit 0
fi

PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$PROJECT_ROOT"

echo "=== Project root ==="
echo "$PROJECT_ROOT"

echo ""
echo "=== Git status ==="
git status --short

echo ""
echo "=== Staged diff ==="
git diff --staged --stat 2>/dev/null || true

echo ""
echo "=== Unstaged diff ==="
git diff --stat 2>/dev/null || true

echo ""
echo "=== Recent commits ==="
git log --oneline -10 2>/dev/null || true

echo ""
echo "=== Changed files (working tree vs HEAD) ==="
git diff --name-only HEAD 2>/dev/null || true
git diff --name-only --staged HEAD 2>/dev/null || true

echo ""
echo "=== Tests ==="
# Load env files so PG tests get DATABASE_URL and ADDRESS_VALIDATOR_API_KEY.
if [ -r /etc/wslcb-licensing-tracker/.env ]; then
    set -a; source /etc/wslcb-licensing-tracker/.env; set +a
fi
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a; source "$PROJECT_ROOT/.env"; set +a
fi
source .venv/bin/activate && python -m pytest tests/ -q --tb=short 2>&1 || true
