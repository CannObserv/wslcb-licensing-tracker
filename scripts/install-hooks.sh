#!/usr/bin/env bash
# Install the pre-commit hook that auto-rebuilds and stages tailwind.css.
#
# Run this from the repository root after cloning:
#   scripts/install-hooks.sh
#
# Git hooks are stored in .git/hooks/ of the main checkout and shared
# automatically across all git worktrees — run this once from the main
# repo, not from within a worktree.
set -euo pipefail

MARKER="# wslcb-licensing-tracker managed hook"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Detect worktree (.git is a file, not a directory)
if [ -f "$ROOT/.git" ]; then
    echo "error: run install-hooks.sh from the main repo checkout, not a worktree." >&2
    echo "       Hooks are stored in the main .git/hooks/ and shared across all worktrees." >&2
    exit 1
fi

HOOK_FILE="$ROOT/.git/hooks/pre-commit"

# If an existing hook was not installed by us, refuse to overwrite.
if [ -f "$HOOK_FILE" ] && ! grep -qF "$MARKER" "$HOOK_FILE"; then
    echo "error: a pre-commit hook already exists at $HOOK_FILE" >&2
    echo "       It was not installed by this script. To integrate manually:" >&2
    echo "       1. Add the following lines to the end of your existing hook:" >&2
    echo '          ROOT="$(git rev-parse --show-toplevel)"' >&2
    echo '          "$ROOT/scripts/build-css.sh" || exit 1' >&2
    echo '          git diff --quiet -- static/css/tailwind.css 2>/dev/null || git add static/css/tailwind.css' >&2
    echo "       That's all — your hook now handles the Tailwind build." >&2
    exit 1
fi

mkdir -p "$(dirname "$HOOK_FILE")"
cat > "$HOOK_FILE" << HOOK
#!/bin/sh
$MARKER
# Auto-rebuild Tailwind CSS before commit; stage if changed.
ROOT="\$(git rev-parse --show-toplevel)"
"\$ROOT/scripts/build-css.sh" || {
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
