#!/usr/bin/env bash
# Download the Tailwind CLI standalone binary for the current platform.
# Output: scripts/bin/tailwindcss  (executable)
# Safe to re-run — exits immediately if binary already exists.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
BINARY="$BIN_DIR/tailwindcss"

if [ -f "$BINARY" ]; then
    exit 0
fi

mkdir -p "$BIN_DIR"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)         ARCH="x64"   ;;
    aarch64|arm64)  ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
case "$OS" in
    linux)  PLATFORM="linux-${ARCH}"  ;;
    darwin) PLATFORM="macos-${ARCH}"  ;;
    *) echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

VERSION="v3.4.17"
URL="https://github.com/tailwindlabs/tailwindcss/releases/download/${VERSION}/tailwindcss-${PLATFORM}"

echo "Downloading Tailwind CLI ${VERSION} for ${PLATFORM}..." >&2
curl -fsSL -o "$BINARY" "$URL"
chmod +x "$BINARY"
echo "Tailwind CLI installed at $BINARY" >&2
