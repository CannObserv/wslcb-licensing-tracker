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
# Each variable is CO_WSLCB_LICENSING_TRACKER_<name> (#204):
#   RELAY_HOST      ssh destination of the VM (required), e.g. wslcb-licensing-tracker.exe.xyz
#   RELAY_INBOX     inbox path on the VM, relative to its home (default wslcb-relay-inbox)
#   RELAY_URL       page to fetch (default: the WSLCB report)
#   RELAY_CHECKOUT  the tracker checkout on the VM, relative to its home
#                   (default wslcb-licensing-tracker)
# The pre-#204 WSLCB_<name> is no longer read (#244).
set -euo pipefail

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) relay: $*"; }

PREFIX=CO_WSLCB_LICENSING_TRACKER_
# prefixed_env NAME [DEFAULT]: $PREFIX<NAME> if set, else DEFAULT.
prefixed_env() {
  local name="$PREFIX$1"
  if [ -n "${!name+x}" ]; then
    printf '%s' "${!name}"
  else
    printf '%s' "${2-}"
  fi
}

HOST=$(prefixed_env RELAY_HOST)
if [ -z "$HOST" ]; then
  log "set ${PREFIX}RELAY_HOST to the ssh destination of the VM" >&2
  exit 1
fi
INBOX=$(prefixed_env RELAY_INBOX wslcb-relay-inbox)
URL=$(prefixed_env RELAY_URL https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp)
UNIT="wslcb-task@import-relayed.service"
CHECKOUT=$(prefixed_env RELAY_CHECKOUT wslcb-licensing-tracker)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

stamp=$(date -u +%Y%m%dT%H%M%SZ)   # the fetch time; the VM dates the capture by it
name="$stamp.html.gz"

# Identify us as every server-side client does (#243). The VM owns the string
# (version + contact email); without a valid one it refuses, and so do we.
ua=$(ssh -o BatchMode=yes "$HOST" \
  "$CHECKOUT/.venv/bin/wslcb ops user-agent --env-file /etc/wslcb-licensing-tracker/.env")
if [ -z "$ua" ]; then
  log "no User-Agent from $HOST; not fetching" >&2
  exit 1
fi

log "fetching $URL"
curl --fail --silent --show-error --max-time 300 --user-agent "$ua" \
  --output "$work/page.html" "$URL"
if [ ! -s "$work/page.html" ]; then
  log "empty response; not relaying" >&2
  exit 1
fi
gzip -c "$work/page.html" > "$work/$name"

log "uploading $name to $HOST:$INBOX"
scp -q -o BatchMode=yes "$work/$name" "$HOST:$INBOX/$name.part"
# The import skips *.part, so it never sees a half-written upload.
# systemctl start returns only a status; the import's per-capture lines
# ("<name>: rejected: <reason>") are in the journal, so fetch this run's
# plain-text ones (not the JSON log lines) and exit with the import's status.
log "importing on $HOST"
ssh -o BatchMode=yes "$HOST" \
  "mv '$INBOX/$name.part' '$INBOX/$name' && t=\$(date +%s) && {
     sudo -n systemctl start $UNIT; rc=\$?
     journalctl -q -o cat --no-pager -u $UNIT --since @\$t | grep -v '^{'
     exit \$rc; }"
log "done: $name"
