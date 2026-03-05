#!/usr/bin/env bash
# check-status.sh
# Reports working tree state. Exits 0 if clean, 1 if there are uncommitted changes.
# Detects the git project root automatically; safe to invoke from any directory.
#
# Usage: bash scripts/check-status.sh [--help]
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  echo "Usage: bash scripts/check-status.sh"
  echo ""
  echo "Reports branch, working tree status, and recent commits."
  echo "Exits 0 if clean, 1 if uncommitted changes are present."
  echo "Automatically resolves the git project root regardless of invocation directory."
  exit 0
fi

PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$PROJECT_ROOT"

echo "=== Branch ==="
git branch --show-current

echo ""
echo "=== Status ==="
git status --short

echo ""
echo "=== Recent commits ==="
git log --oneline -5

if [ -n "$(git status --porcelain)" ]; then
  echo ""
  echo "UNCOMMITTED CHANGES DETECTED"
  exit 1
fi

echo ""
echo "Working tree is clean."
