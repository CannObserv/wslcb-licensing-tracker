#!/usr/bin/env bash
# comment-issue.sh <issue-number> <comment-body>
# Posts a comment to a GitHub issue via gh CLI.
#
# Usage: bash scripts/comment-issue.sh [--help]
#        bash scripts/comment-issue.sh <number> <body>
set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
  echo "Usage: bash scripts/comment-issue.sh <issue-number> <comment-body>"
  echo ""
  echo "Posts a comment to a GitHub issue via the gh CLI."
  echo ""
  echo "Arguments:"
  echo "  issue-number   GitHub issue number (e.g. 19)"
  echo "  comment-body   Comment text (quote multi-word strings)"
  echo ""
  echo "Example:"
  echo "  bash scripts/comment-issue.sh 19 \"Implemented X. Commits: abc123..def456\""
  echo ""
  echo "Exit codes:"
  echo "  0  Comment posted successfully"
  echo "  1  Missing arguments or gh CLI error"
  exit 0
fi

ISSUE="${1:-}"
BODY="${2:-}"

if [ -z "$ISSUE" ] || [ -z "$BODY" ]; then
  echo "Error: issue number and comment body are required."
  echo "Run with --help for usage."
  exit 1
fi

gh issue comment "$ISSUE" --body "$BODY"
echo "Comment posted to issue #$ISSUE."
