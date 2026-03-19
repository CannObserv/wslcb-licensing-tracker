#!/usr/bin/env bash
# Rebuild Tailwind CSS and auto-stage the output if it changed.
# Registered as a local hook in .pre-commit-config.yaml — do not run directly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/build-css.sh" || exit 1

if ! git diff --quiet -- static/css/tailwind.css 2>/dev/null; then
    git add static/css/tailwind.css
    echo "[pre-commit] auto-staged regenerated static/css/tailwind.css"
fi
