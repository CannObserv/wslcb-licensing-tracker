#!/usr/bin/env bash
# close-issue.sh <issue-number>
# Closes a GitHub issue via gh CLI.
#
# Usage: bash scripts/close-issue.sh [--help]
#        bash scripts/close-issue.sh <number>
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  echo "Usage: bash scripts/close-issue.sh <issue-number>"
  echo ""
  echo "Closes a GitHub issue via the gh CLI."
  echo ""
  echo "Arguments:"
  echo "  issue-number   GitHub issue number (e.g. 19)"
  echo ""
  echo "Example:"
  echo "  bash scripts/close-issue.sh 19"
  echo ""
  echo "Exit codes:"
  echo "  0  Issue closed successfully"
  echo "  1  Missing argument or gh CLI error"
  exit 0
fi

ISSUE="${1:-}"

if [ -z "$ISSUE" ]; then
  echo "Error: issue number is required."
  echo "Run with --help for usage."
  exit 1
fi

gh issue close "$ISSUE"
echo "Issue #$ISSUE closed."
