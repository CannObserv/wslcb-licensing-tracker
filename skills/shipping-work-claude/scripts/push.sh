#!/usr/bin/env bash
# push.sh
# Pushes the current branch to origin.
# Detects the git project root automatically; safe to invoke from any directory.
#
# Usage: bash scripts/push.sh [--help]
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  echo "Usage: bash scripts/push.sh"
  echo ""
  echo "Pushes the current git branch to origin."
  echo "Automatically resolves the git project root regardless of invocation directory."
  exit 0
fi

PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$PROJECT_ROOT"

BRANCH=$(git branch --show-current)
echo "Pushing $BRANCH to origin..."
git push origin "$BRANCH"
echo "Push succeeded."
