# Relayed scrape

A stopgap while `licensinginfo.lcb.wa.gov` blocks this host (#198): the
operator's Mac fetches the page and relays it in (#201). Design:
[`specs/2026-10-20-relayed-scrape-design.md`](specs/2026-10-20-relayed-scrape-design.md).

## Install

The relay runs at 00:40 and 06:40 Mac-local time (Pacific assumed), 10 min
after the VM's own scrape. `scripts/relay/relay-capture.sh` does the following:

1. `curl`s the page.
2. Gzips it as `<UTC fetch time>.html.gz`.
3. `scp`s it to the inbox as `.part`.
4. Renames it into place and runs
   `sudo -n systemctl start wslcb-task@import-relayed`, waiting for it so the
   Mac sees the import's result.

The import (`wslcb ingest import-relayed`) ingests through the scrape's own path
under the `relayed_scrape` source type ("Relayed Capture" on record pages). How
it reads in `/health` and co-status: § Monitoring below.

**VM, once:** `mkdir -p ~/wslcb-relay-inbox` (or set `WSLCB_RELAY_INBOX` in
`/etc/wslcb-licensing-tracker/.env`; keep it outside `data/`, which the backup
mirrors), plus the [`DEPLOYMENT.md`](DEPLOYMENT.md) copy block, which installs the
`wslcb-task@import-relayed.service.d/` drop-in.

**Mac, once,** from a checkout of this repo. Use a key of its own, so it can be
revoked alone. A LaunchAgent (not cron) reaches the login session's ssh-agent
and Keychain.

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_wslcb_relay -C wslcb-relay
# add ~/.ssh/id_ed25519_wslcb_relay.pub to the exe.dev account, then:
ssh-add --apple-use-keychain ~/.ssh/id_ed25519_wslcb_relay
cat >> ~/.ssh/config <<'CFG'
Host wslcb-relay
  HostName wslcb-licensing-tracker.exe.xyz
  IdentityFile ~/.ssh/id_ed25519_wslcb_relay
  IdentitiesOnly yes
  UseKeychain yes
  AddKeysToAgent yes
CFG
ssh wslcb-relay true                      # accept the host key once, by hand
mkdir -p ~/.local/bin
install -m 0755 scripts/relay/relay-capture.sh ~/.local/bin/wslcb-relay-capture.sh
sed -e "s|@HOME@|$HOME|g" -e "s|@HOST@|wslcb-relay|g" scripts/relay/wslcb-relay.plist \
  > ~/Library/LaunchAgents/org.cannobserv.wslcb-relay.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.cannobserv.wslcb-relay.plist
launchctl kickstart -p gui/$(id -u)/org.cannobserv.wslcb-relay   # one run now
tail -f ~/Library/Logs/wslcb-relay.log    # ends "relay: done: <name>"
```

The VM's copy of each run is in `journalctl -u wslcb-task@import-relayed`. A
run by hand (`WSLCB_RELAY_HOST=wslcb-relay ~/.local/bin/wslcb-relay-capture.sh`)
works the same way.

**Retire** when co-status reports the scrape fault *has cleared*:
`launchctl bootout gui/$(id -u)/org.cannobserv.wslcb-relay`. The command, the
source type and the script stay in the repo for the next block.

## Monitoring

- **No check-in.** `wslcb-task@import-relayed` has no credential and never
  posts. `co-wslcb-scrape` keeps reporting the *server's* 403 fault, one
  reminder a day. Its next *has cleared* means the block lifted, and that's
  the cue to unload the LaunchAgent. If relays checked in, they would hide
  that signal.
- **`/health` reads relays as fresh data.** `last_success_at`, `stale` and the
  alert's `window_expires` take the latest success of either path, dated by
  the relay's `scrape_log.captured_at` (the Mac's fetch time).
  `consecutive_errors` and `last_attempt_status` read `origin = 'server'` rows
  only, so a relay neither resets nor adds to the server's count.
- **A relay that stops is not paged.** It surfaces only as `/health` going
  stale after `STALE_AFTER`. Check that relays are arriving with
  `journalctl -u wslcb-task@import-relayed`, or `SELECT … FROM scrape_log
  WHERE origin = 'relay'`.
- **A rejected capture** (over 6 h old, from the future, not gzip, under
  1 MB, no `</html>`, a section missing, or no records) gets an `error` row
  with `origin = 'relay'` and its reason, is moved to
  `~/wslcb-relay-inbox/rejected/`, and makes the import and the Mac's script
  exit non-zero. Look there, and in `~/Library/Logs/wslcb-relay.log` on the
  Mac.
- **Any other failure** (a DB fault, say) is not the capture's. Its row is
  marked `error`, the file stays queued in the inbox, and the run fails. To
  retry once fixed, run `sudo systemctl start wslcb-task@import-relayed`
  within the 6 h age limit.
