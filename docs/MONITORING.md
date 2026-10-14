# Monitoring

Dead-man check-ins to co-status (CannObserv/status), over the tailnet
(`tag:wslcb → tag:status:9000`; [`RECOVERY.md`](RECOVERY.md) § Tailnet). One
module, `checkin.py`, one `Monitor` per job; both on tenant `co-wslcb`.

| Job | Monitor | Runbook |
|---|---|---|
| `wslcb-backup.service` | `co-wslcb-backup` | [`RECOVERY.md`](RECOVERY.md) § The co-status monitor |
| `wslcb-task@scrape.service` | `co-wslcb-scrape` | below |

## Scrape check-in (#196)

Each scrape checks in to co-status, as the backup does ([`RECOVERY.md`](RECOVERY.md)
§ The co-status monitor; same module, `checkin.py`). Nothing else notices a
failed scrape: from 2026-10-13 a WSLCB block 403'd every run for a day before
anyone looked (#198).

- **When:** right after `scraper.scrape()` returns or raises, before the
  post-scrape address backfill, which takes minutes and cannot fail the scrape.
- **`ok`** after `success` or `unchanged`. **`alert`** on any exception; the
  run still exits 1, so the unit still fails. co-status folds consecutive
  alerts into one fault (one report, a daily reminder, *has cleared* on the
  next `ok`), and `metadata.fault` (`HTTPStatusError 403`, `ConnectError`, …)
  reports a *different* failure at once.
- **Silence** alarms too: a stopped timer, a masked unit, a run hung until
  its timeout.
- Config: `WSLCB_SCRAPE_CHECKIN_BASE_URL` and `WSLCB_SCRAPE_MONITOR_ID` in
  `/etc/wslcb-licensing-tracker/.env` (the template's `EnvironmentFile`); the
  key is the drop-in's `checkin-key` credential, never an env var. A manual
  `wslcb ingest scrape` has no credential and warns that it isn't checking in.

**The monitor** is `co-wslcb-scrape`, on tenant `co-wslcb` (CannObserv/status#46):

| Field | Value | Why |
|---|---|---|
| `interval_seconds` | `86400` | runs are 6 h then 18 h apart; a full cycle is 24 h, and anything shorter alarms on one missed morning run |
| `grace_seconds` | `7200` | 5 min jitter, plus the scrape before the check-in (max 61 s measured), plus 1 h: the timer is Pacific time, so the fall-back makes one cycle a year 25 h |
| `renotify_seconds` | `86400` | one reminder a day while a fault lasts |
| `title_template` | `wslcb scrape {{ outcome }} on {{ source_host }}` | rendered for an `alert` only |
| `body_template` | `{{ error }} — {{ consecutive_errors }} consecutive; last success {{ last_success_at }}; the source window starts dropping uncaptured records after {{ window_expires }}` | as requested in status#46 |

`tests/test_infra_scrape_unit.py` holds the interval and grace above to the
timer. An `alert` carries `outcome` (`failed`), `source_host`, `error`,
`consecutive_errors`, `last_success_at` and `window_expires`: the last success
plus 30 days, after which the source window drops records the gap missed.
All of them are always present; the last three are `""` when the database
can't be read. An `ok` carries `outcome`, `source_host`, `scrape_status`,
`records_{new,approved,discontinued,skipped}` and `scrape_log_id`.

**Install, in this order.** The key file first: a missing `LoadCredential=`
source fails every scrape (243/CREDENTIALS).

```bash
sudo install -m 0400 -o root -g root /dev/null /etc/wslcb-licensing-tracker/scrape-checkin.key
sudo cp -r infra/wslcb-task@scrape.service.d /etc/systemd/system/
sudo systemctl daemon-reload
```

Then, once the monitor id is posted. The key below is the `co-wslcb` tenant's,
the one the backup already holds, copied root to root and never shown. That
holds **if status#46 confirms keys are tenant-scoped**. If they are per
monitor, write the new key in at a prompt instead, as RECOVERY.md § Install
and first run does for the backup's; a wrong key reads as `rejected with HTTP 403`.

```bash
sudo install -m 0400 -o root -g root /etc/wslcb-licensing-tracker/backup-checkin.key \
     /etc/wslcb-licensing-tracker/scrape-checkin.key
# The leading \n: a file whose last line has no newline would fuse with the first.
printf '\nWSLCB_SCRAPE_CHECKIN_BASE_URL=http://status:9000\nWSLCB_SCRAPE_MONITOR_ID=<id>\n' \
  | sudo tee -a /etc/wslcb-licensing-tracker/.env >/dev/null
sudo systemctl start wslcb-task@scrape.service   # optional: check in now, not at the next firing
```

The run reads as `exedev`, so code running as that user can read the key's
private copy during a run. That adds no exposure: `exedev` already owns the
checkout and venv the backup runs. Until all three are set the run logs `scrape
check-in not configured` (a WARNING) or `half-configured` (an ERROR). A landed
check-in shows only as httpx's `"HTTP/1.1 202 Accepted"`; the proof is the
monitor's fresh last check-in.
