#!/usr/bin/env bash
# Build static/css/tailwind.css from templates.
# Auto-downloads the CLI binary if not present.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Auto-download binary if missing
"$SCRIPT_DIR/download-tailwind.sh"

BINARY="$SCRIPT_DIR/bin/tailwindcss"
"$BINARY" \
    -c "$ROOT/tailwind.config.js" \
    -i "$ROOT/static/css/input.css" \
    -o "$ROOT/static/css/tailwind.css" \
    --minify
echo "Built $ROOT/static/css/tailwind.css" >&2
