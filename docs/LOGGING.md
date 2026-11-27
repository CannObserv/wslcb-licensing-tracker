# Logging

The rules every module follows are in [`AGENTS.md`](../AGENTS.md#logging). The JSON-lines shape under systemd and the `journalctl` commands are in [`DEPLOYMENT.md`](DEPLOYMENT.md#logging). This doc holds the uvicorn detail behind the `--log-config` rule.

It also holds the persistent journal (#186) and PID 1's log target (#254).

## uvicorn log routing

- **uvicorn:** every uvicorn invocation (systemd `ExecStart` + all dev-server commands) must pass `--log-config src/wslcb_licensing_tracker/log_config.json`, or uvicorn's own `uvicorn`/`uvicorn.access`/`uvicorn.error` lines bypass the JSON formatter and journald gets mixed plain-text/JSON logs (#162). That file and `setup_logging()` share one formatter via `build_json_formatter()` — change the JSON schema in that factory only. `log_config.json` also attaches `ColorMessageFilter` to every uvicorn logger, stripping uvicorn's ANSI-coloured `color_message` extra at the record (#163) — keep it on the *loggers*, not the handler or the formatter's `reserved_attrs`: a filter placed on one sink is resurrected by the next one, and a logger never inherits an ancestor's filters.

## Journal

Persistent since 2026-10-28 (#186), capped at 512M. The Feb-2026 exeuntu image masks
`systemd-journal-flush.service`, so journald stayed in `/run` despite the
image's `Storage=persistent`, and the #184 and #220 reboots each lost a boot.
The fix is the **unmask**: systemd ignores drop-ins on a masked unit, and
`sysinit.target.wants/` already pulls the vendor unit in (`is-enabled` then
reads `static`). `infra/journald.conf.d/wslcb.conf` tracks the setting and the
cap. #200's disk-gc layer adds `zz-cannobserv-journal.conf` with the same 512M
but no `Storage=`, so its cap duplicates ours while our `Storage=persistent`
stays the repo's only tracked copy: keep the file.

```bash
sudo install -m 644 infra/journald.conf.d/wslcb.conf /etc/systemd/journald.conf.d/
sudo systemctl unmask systemd-journal-flush.service
sudo systemctl restart systemd-journald     # stdout streams survive: fd store
sudo systemctl start systemd-journal-flush.service
```

- **Verify:** the flush is `static` and `active`, `/run/systemd/journal/flushed`
  exists, `/var/log/journal/<machine-id>/system.journal` starts at the boot's
  first line, `journalctl --disk-usage` < 512M, a `curl` of `/api/v1/health`
  logs a fresh `uvicorn.access` line, and the probe's `journal.verdict` is
  `persistent`. `tests/test_journald_config.py` passes on the host, 0 skipped.
  `wslcb-web` and `wslcb-task@` run as `exedev`, so their lines land in
  `user-1000.journal`, not `system.journal`; `journalctl -u` reads both.
  Reboot survival shows only after the next one: `journalctl --list-boots` > 1
  boot, `journalctl -b -1 -n 5` ends in `Journal stopped`.
- **Retention:** ~1.5 MB/day (16M in 10.4 d, 24M in 17 d), so 512M ≈ a year.
- **Personal data** (2026-10-28): no personal email addresses (only unit names
  and kernel boot lines).
  `wslcb-web` access lines carry the client socket (127.0.0.1, bar one) and the
  path and query string of searches over public data, kept as long as the
  journal. `sudo journalctl --vacuum-time=<age>` shortens it.
- **Re-mask:** only a rebuild from the Feb-2026 image; dpkg never writes masks
  to `/etc/systemd/system`. The host test and the probe (`journal:volatile`)
  both catch it.

### PID 1's log target

Installed 2026-11-18 (#254). The image's `system.conf.d/container-overrides.conf`
sets `LogTarget=console`, which the first `daemon-reload` after boot applies:
until the next boot, systemd's own `Started` / `Finished` / `Failed with
result` lines never reach `journalctl -u`. `infra/system.conf.d/zz-cannobserv-log-target.conf`
sorts after it and sets only `LogTarget=journal-or-kmsg`. It is
CannObserv/provisioner#62's, byte for byte at `d2bdd36` (the `#62` in its
comment is provisioner's issue, not ours): re-copy, never edit.
`tests/test_log_target_config.py` pins hash, setting, sort order, installed copy.
From boot until the first reload it reads `journal`: exe-init starts PID 1 with
`--log-target=syslog` (`/proc/1/cmdline`), which systemd reads as `journal` and
which beats `system.conf.d` until a reload applies the file (measured on #255).
Both keep PID 1's lines in the journal; only `console` loses them.

```bash
sudo install -o root -g root -m 0644 infra/system.conf.d/zz-cannobserv-log-target.conf /etc/systemd/system.conf.d/
sudo systemctl daemon-reload            # outside the knob's quiet ranges
systemd-analyze get-log-target          # journal-or-kmsg after a reload; journal from boot until one
sudo systemd-run --unit=log-target-check --quiet true && sudo journalctl -u log-target-check   # Started + Deactivated
# revert: sudo rm /etc/systemd/system.conf.d/zz-cannobserv-log-target.conf && sudo systemctl daemon-reload
```

Still `console`? A runtime `set-log-target` survives reloads until boot
(CannObserv/usa-wa#520): `sudo journalctl _PID=1 | grep 'Setting log target'`.
`sudo kill -s RTMIN+26 1` restores the configured target (systemd(1)); reload
again. Never `set-log-target` here: it is itself an override, so the file is
then ignored until boot.
