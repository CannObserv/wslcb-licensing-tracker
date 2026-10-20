#!/bin/bash
# Relay one capture of the WSLCB page to the tracker VM (#201).
#
# Runs on the operator's Mac while licensinginfo.lcb.wa.gov blocks the VM
# (#198): fetch the page with curl (never a browser "Save as" — browsers
# rewrite HTML), gzip it under its UTC fetch time, upload it to the VM's relay
# inbox as *.part, rename it into place, then run the import and wait for it.
# Exits non-zero when the fetch, the upload, or the import fails. Written for
# macOS's BSD userland; launchd runs it via wslcb-relay.plist.
#
#   WSLCB_RELAY_HOST   ssh destination of the VM (required), e.g. wslcb-licensing-tracker.exe.xyz
#   WSLCB_RELAY_INBOX  inbox path on the VM, relative to its home (default wslcb-relay-inbox)
#   WSLCB_RELAY_URL    page to fetch (default: the WSLCB report)
set -euo pipefail

HOST="${WSLCB_RELAY_HOST:?set WSLCB_RELAY_HOST to the ssh destination of the VM}"
INBOX="${WSLCB_RELAY_INBOX:-wslcb-relay-inbox}"
URL="${WSLCB_RELAY_URL:-https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp}"
UNIT="wslcb-task@import-relayed.service"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) relay: $*"; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

stamp=$(date -u +%Y%m%dT%H%M%SZ)   # the fetch time; the VM dates the capture by it
name="$stamp.html.gz"

log "fetching $URL"
curl --fail --silent --show-error --max-time 300 --output "$work/page.html" "$URL"
if [ ! -s "$work/page.html" ]; then
  log "empty response; not relaying" >&2
  exit 1
fi
gzip -c "$work/page.html" > "$work/$name"

log "uploading $name to $HOST:$INBOX"
scp -q -o BatchMode=yes "$work/$name" "$HOST:$INBOX/$name.part"
# The import skips *.part, so it never sees a half-written upload.
log "importing on $HOST"
ssh -o BatchMode=yes "$HOST" \
  "mv '$INBOX/$name.part' '$INBOX/$name' && sudo -n systemctl start $UNIT"
log "done: $name"
