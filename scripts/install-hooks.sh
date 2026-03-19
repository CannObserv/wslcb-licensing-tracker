#!/usr/bin/env bash
# Install the pre-commit hook that auto-rebuilds and stages tailwind.css.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOK_FILE="$ROOT/.git/hooks/pre-commit"

cat > "$HOOK_FILE" << 'HOOK'
#!/bin/sh
# Auto-rebuild Tailwind CSS before commit; stage if changed.
ROOT="$(git rev-parse --show-toplevel)"
"$ROOT/scripts/build-css.sh" || {
    echo "[pre-commit] Tailwind build failed — commit aborted." >&2
    exit 1
}
if ! git diff --quiet -- static/css/tailwind.css 2>/dev/null; then
    git add static/css/tailwind.css
    echo "[pre-commit] auto-staged regenerated static/css/tailwind.css"
fi
HOOK

chmod +x "$HOOK_FILE"
echo "Installed pre-commit hook at $HOOK_FILE" >&2
