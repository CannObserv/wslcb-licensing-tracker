# Monitoring

Dead-man check-ins to co-status (CannObserv/status), over the tailnet
(`tag:wslcb → tag:status:9000`; [`RECOVERY.md`](RECOVERY.md) § Tailnet). One
module, `checkin.py`, one `Monitor` per job; all on tenant `co-wslcb`.

| Job | Monitor | Runbook |
|---|---|---|
| `wslcb-backup.service` | `co-wslcb-backup` | [`RECOVERY.md`](RECOVERY.md) § The co-status monitor |
| `wslcb-task@scrape.service` | `co-wslcb-scrape` | below |
| `wslcb-entity-classification.timer` | `co-wslcb-classify` | [below](#entity-classification-check-in-207) |
| `cannobserv-disk-headroom.timer` | `co-wslcb-disk-headroom` | [`DISK_GC.md`](DISK_GC.md): the cohort GC's own sensor, not `checkin.py` (#200) |

All four check in with one tenant key, in four copies: rotate or restore them
together ([§ Key rotation](#key-rotation-241)).

**A rejected check-in** logs `<job> check-in rejected with HTTP <code>`, with
what the code means at co-status and co-status's own response body (one line,
capped at 300 characters; never the request headers), for every monitor. None
is retried, and none changes the job's exit status (#199):

| Code | Hint logged | Means |
|---|---|---|
| 401 / 403 / 404 | `check the monitor id and key` | bad key, wrong tenant, unknown monitor id |
| 422 | `the monitor's templates could not render these variables, or the payload is not valid JSON …; nothing was recorded` | the payload was refused: no event, and the timer isn't reset. The body names the field or template section. co-status renders only an `alert` that is due a report (the fault's first, or a reminder), so a repeat inside a fault can't catch a template a variable breaks |
| other 4xx | none | |

A 5xx or a transport error is retried once, then logged as `check-in failed`.

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
  next `ok`), and `metadata.fault` (`HTTPStatusError 403`, `ConnectError`,
  `ContactEmailError` for a missing contact email (#243), …) reports a
  *different* failure at once.
- **Silence** alarms too: a stopped timer, a masked unit, a run hung until
  its timeout.
- Config: `WSLCB_SCRAPE_CHECKIN_BASE_URL` and `WSLCB_SCRAPE_MONITOR_ID` in
  `/etc/wslcb-licensing-tracker/.env` (the template's `EnvironmentFile`); the
  key is the drop-in's `checkin-key` credential, never an env var. A manual
  `wslcb ingest scrape` has no credential and warns that it isn't checking in.
- **Contact email (#243):** `CO_WSLCB_LICENSING_TRACKER_CONTACT_EMAIL` goes in
  the app `.env` **and** `backup.env` (rules in `.env.example`). Check it with
  `wslcb ops user-agent --env-file /etc/wslcb-licensing-tracker/.env`. Without
  it the scrape fails with `ContactEmailError`. Check-ins still go, without
  the contact.

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
All of them are always present, never blank (#199). The last three read
`unknown` when the database can't be read. With no success on record,
`consecutive_errors` stays the real count, `last_success_at` reads `never` and
`window_expires` reads `n/a`. An `ok` carries `outcome`, `source_host`, `scrape_status`,
`records_{new,approved,discontinued,skipped}` and `scrape_log_id`.

**Install, in this order.** The key file first: a missing `LoadCredential=`
source fails every scrape (243/CREDENTIALS).

```bash
sudo install -m 0400 -o root -g root /dev/null /etc/wslcb-licensing-tracker/scrape-checkin.key
sudo cp -r infra/wslcb-task@scrape.service.d /etc/systemd/system/
sudo systemctl daemon-reload
```

Then, once the monitor id is posted. The key below is the `co-wslcb` tenant's,
the one the backup already holds, copied root to root and never shown: keys
are tenant-scoped (status#46, from `_load_owned`). An unknown or revoked key
reads as `rejected with HTTP 401`.

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

**Freshness on demand (#197).** The check-in raises the alarm; `GET
/api/v1/health` shows the current state to anyone who looks. Its
`data.scrape` carries `last_success_at`, `last_attempt_status`,
`consecutive_errors` and `stale`, true past `STALE_AFTER` (26 h, this
monitor's interval plus grace, held equal by `tests/test_infra_scrape_unit.py`)
or with no successful run ever. `/health` stays 200 when stale:
`wslcb-healthcheck` restarts the web app on any non-2xx, and a restart can't
fix an upstream outage. `GET /api/v1/health/ingest` returns the same object
with a 503 when stale, for an external uptime check; never point
`wslcb-healthcheck` at it. If `scrape_log` can't be read, `data.scrape` is
null (`/health` still 200, "scrape freshness unknown") and `/health/ingest`
503s "Scrape freshness unavailable" with `db: ok`.

## Entity classification check-in (#207)

Jev classification ([`DEPLOYMENT.md`](DEPLOYMENT.md) § Entity classification)
must not stop unnoticed: a missing key, a vendor outage or a dead timer all
look like nothing happening. One monitor, `co-wslcb-classify`.

- **The weekly timer is the only `ok` sender.**
  `wslcb-entity-classification.timer` runs `wslcb classify-entities` in its
  default mode, then checks in.
  - **`ok`** on a clean run. With the flag off it still runs and sends `ok`
    with `enabled=false`, so the dead-man stays armed.
  - **`alert`**, with `metadata.fault`, when any of these happens:
    - `missing_api_key`: the flag is on and the key is unset;
    - `auth_failed`: a 401/403 stopped the run at once;
    - `consecutive_failures`: three failed batches in a row;
    - `batches_failed`: any other failed batch;
    - `backlog`: more than 1,000 entities still await a first answer;
    - `locked`: another run held the lock, so this week's run did nothing. A
      lock that never clears would otherwise be silent. Exit status stays 0.
  - **A run that raises** alerts with the exception type, and still exits 1.
  - `--dry-run` and the refresh options never check in. `--limit` alone is
    still the default mode (the one-off backfill), and checks in when the
    run has the credential.
- **Post-scrape passes send `alert` only, never `ok`.** Twice-daily `ok`s would
  mask a dead timer. A post-scrape run that loses the lock is silent: a manual
  backfill holding it is no fault. The post-scrape alert goes with the scrape
  unit's own `checkin-key`. That works because check-in keys are
  tenant-scoped (CannObserv/status#46, from `_load_owned`).
- **The relay import never checks in.** `wslcb-task@import-relayed` has no
  credential, and must not get one ([§ Relayed scrape](#relayed-scrape-201)).
  Its failures show in the journal and in `classification_runs`. The weekly
  run catches its backlog.
- **While WSLCB blocks the host (#198)** the scrape raises before its hook, so
  every post-ingest pass is the relay's: a failure after ingest surfaces
  only at the weekly run.
- Config: `CO_WSLCB_LICENSING_TRACKER_CLASSIFY_CHECKIN_BASE_URL` and
  `CO_WSLCB_LICENSING_TRACKER_CLASSIFY_MONITOR_ID` in
  `/etc/wslcb-licensing-tracker/.env`. The key is the drop-in's `checkin-key`
  credential. Both the classify and scrape units read these variables (the
  scrape's for its alerts).

**The monitor** is `co-wslcb-classify`, on tenant `co-wslcb`:

| Field | Value | Why |
|---|---|---|
| `interval_seconds` | `604800` | one run a week |
| `grace_seconds` | `10800` | 5 min jitter, plus 1 h for the fall-back (the timer is Pacific time), plus the run: up to its 1 h `TimeoutStartSec` |
| `renotify_seconds` | `86400` | one reminder a day while a fault lasts |
| `title_template` | `wslcb classify {{ outcome }} on {{ source_host }}` | rendered for an `alert` only |
| `body_template` | `{{ error }} — enabled={{ enabled }}, {{ batches_failed }} failed batch(es), backlog {{ backlog }}` | |

`tests/test_infra_classify_unit.py` holds the interval and grace above to the
timer and the drop-in.

**Variables.** Every check-in carries all of these:
- `outcome`: `ok` or `failed`;
- `source_host`;
- `enabled`: `true` or `false`;
- `model`;
- `names_sent`, `batches_ok`, `batches_failed`;
- `backlog`: entities awaiting a first answer, counted after the run. It is
  `n/a` on post-scrape alerts, which don't count it;
- `error`: the fault, or `""`.

An alert for a raised run has `error` = `Type: first line`, and its four counts
read `n/a`. Every variable is always sent: a missing one is a 422 and the
check-in is lost.

**Install, in this order.** The key file first: a missing `LoadCredential=`
source fails every run (243/CREDENTIALS).

```bash
sudo install -m 0400 -o root -g root /dev/null /etc/wslcb-licensing-tracker/classify-checkin.key
sudo cp -r 'infra/wslcb-task@classify-entities.service.d' /etc/systemd/system/
sudo cp infra/wslcb-entity-classification.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wslcb-entity-classification.timer
```

Then, once the monitor id is posted, copy the tenant key the backup already
holds, root to root and never shown. Both units read the same two variables:

```bash
sudo install -m 0400 -o root -g root /etc/wslcb-licensing-tracker/backup-checkin.key \
     /etc/wslcb-licensing-tracker/classify-checkin.key
# The leading \n: a file whose last line has no newline would fuse with the first.
printf '\nCO_WSLCB_LICENSING_TRACKER_CLASSIFY_CHECKIN_BASE_URL=http://status:9000\nCO_WSLCB_LICENSING_TRACKER_CLASSIFY_MONITOR_ID=<id>\n' \
  | sudo tee -a /etc/wslcb-licensing-tracker/.env >/dev/null
sudo systemctl start 'wslcb-task@classify-entities.service'   # optional: check in now
```

**Set both variables before turning the flag on.** Until then, a post-scrape
alert finds the scrape's key but no base URL or monitor id, and logs
`classify check-in half-configured` (an ERROR) instead of alerting.

## Relayed scrape (#201)

`wslcb-task@import-relayed` never checks in. `co-wslcb-scrape` keeps reporting
the server's own fault until WSLCB lifts its block (#198). `/health` counts a
relay's success as fresh data, but never as a server attempt. Details:
[`RELAY.md`](RELAY.md) § Monitoring.

## Key rotation (#241)

Tenant `co-wslcb` has one check-in key, `co-wslcb-checkin` (keys are
tenant-scoped, status#46). This host holds four root-owned copies of it, one
per unit, each loaded as `LoadCredential=checkin-key:`. Keep them
byte-identical: a rotation or a rebuild that misses one leaves its monitor
unable to check in.

| Copy | Mode | Loaded by | If the file is missing |
|---|---|---|---|
| `/etc/wslcb-licensing-tracker/backup-checkin.key` | 0400 | `wslcb-backup.service` | the start fails (243/CREDENTIALS): no backup |
| `/etc/wslcb-licensing-tracker/scrape-checkin.key` | 0400 | `wslcb-task@scrape.service` (drop-in) | the start fails: no server scrape (the relay import, #201, needs no key) |
| `/etc/wslcb-licensing-tracker/classify-checkin.key` | 0400 | `wslcb-task@classify-entities.service` (drop-in) | the start fails: no weekly classification |
| `/etc/cannobserv/disk-gc/checkin.key` | 0600 | `cannobserv-disk-headroom.service`, the installed GC layer ([`DISK_GC.md`](DISK_GC.md)) | it runs, logs `no checkin-key credential: not checking in`, and goes quiet |

The last is the easy one to miss. It lives outside `/etc/wslcb-licensing-tracker/`,
and its unit falls back to `SetCredential=checkin-key:\n`, so a missing file
fails nothing. Either way co-status alarms on the silence once the monitor's
interval and grace run out. An *empty* file isn't missing: every unit starts,
the other three log `check-in half-configured` (their base URL and monitor id
are set) and the sensor logs the same warning, but nothing checks in.
`tests/test_docs_checkin_keys.py` holds this table to `infra/`.

All four units are `Type=oneshot`, and systemd reads a credential at each
start, so a new key takes effect at each unit's next run. No restart, no
`daemon-reload`.

**Routine: no gap.** co-status can hold two keys on one tenant (its
`scripts/rotate_key.py`: `--new-label` attaches, `--revoke <key-id>` revokes).
Ask its operator for a new key attached to `co-wslcb`, with the old one kept.
Write the new key into the first copy once: read at a prompt, so it reaches
neither history nor argv, and through `tee`, so the file keeps its mode. Then
copy it root to root into the other three, each at its own mode. Never `cat`
or `echo` the key, and never pass it as an argument.

```bash
read -rsp 'new check-in key: ' KEY; echo
printf '%s' "$KEY" | sudo tee /etc/wslcb-licensing-tracker/backup-checkin.key >/dev/null; unset KEY
sudo install -m 0400 -o root -g root /etc/wslcb-licensing-tracker/backup-checkin.key \
     /etc/wslcb-licensing-tracker/scrape-checkin.key
sudo install -m 0400 -o root -g root /etc/wslcb-licensing-tracker/backup-checkin.key \
     /etc/wslcb-licensing-tracker/classify-checkin.key
sudo install -m 0600 -o root -g root /etc/wslcb-licensing-tracker/backup-checkin.key \
     /etc/cannobserv/disk-gc/checkin.key
```

Then run the check below, and only then ask for the old key to be revoked, by
id. `rotate_key.py` prints its `last_used_at` before revoking it: once all four
copies hold the new key, that stops moving.

**Exposure: a gap.** A leaked key is rotated in one transaction (`--new-label`
and `--revoke` together). From then on, every copy still holding the old key
gets a 401, so run the recipe above at once. Until it's done, the three
`checkin.py` units log `check-in rejected with HTTP 401 — check the monitor id
and key`, and the sensor logs `check-in ok (ok) not sent: HTTP 401`. Nothing
is retried.

**The check.** First the copies. The command below compares the hashes without
printing them, then shows each file's mode:

```bash
K='/etc/wslcb-licensing-tracker/backup-checkin.key /etc/wslcb-licensing-tracker/scrape-checkin.key
   /etc/wslcb-licensing-tracker/classify-checkin.key /etc/cannobserv/disk-gc/checkin.key'
sudo sha256sum $K | awk '{print $1}' | sort | uniq -c | awk '{print $1}'   # 4: one hash, all four copies
sudo stat -c '%a %U:%G %s %n' $K   # 400 ×3 and 600, all root:root, none 0 bytes
```

Then each unit's first check-in after the rotation. A 202 proves the key,
whatever the check-in reports: the scrape's are `alert`s while WSLCB blocks the
host (#198). The `checkin.py` units log no success line of their own; a
landed check-in shows only as httpx's `"HTTP/1.1 202 Accepted"`. The sensor
logs its own.

| Unit | Runs | Its 202 |
|---|---|---|
| `cannobserv-disk-headroom` | every 10 min | `checked in ok (ok): HTTP 202` (or `alert (<kind>)`) |
| `wslcb-task@scrape` | twice a day | `"HTTP/1.1 202 Accepted"` |
| `wslcb-backup` | daily | `"HTTP/1.1 202 Accepted"` |
| `wslcb-task@classify-entities` | weekly | `"HTTP/1.1 202 Accepted"` |

```bash
sudo journalctl -u cannobserv-disk-headroom -u wslcb-task@scrape -u wslcb-backup \
     -u wslcb-task@classify-entities --since '<rotation, UTC>' -o short-iso --no-pager \
     -g '202 Accepted|HTTP 202|rejected|check-in failed|not sent|not checking in|configured'   # sudo: else "-- No entries --"
```

The old key can go once the hashes agree and both the sensor and the scrape
have logged a 202. Start the scrape by hand rather than wait for it
(`sudo systemctl start --no-block wslcb-task@scrape.service`). While WSLCB
blocks the host that's one more 403'd attempt, and still a 202. The other two
are proven by their hashes:
- **Backup:** a manual start runs a whole backup, so let the timer's run log
  its 202 within a day.
- **Classify:** a manual start is the timer's paid run, so let the weekly run
  log its 202.

**Rebuild.** [`RECOVERY.md`](RECOVERY.md)'s check-in step writes
`backup-checkin.key`, and the three `install` lines above restore the other
three from it. Until then, each copy behaves as in the table: a missing one
fails its scrape or classify start, and an empty one (the placeholders that §
Scrape check-in and § Entity classification check-in create first) checks in
nothing. The disk-gc copy's `install` needs the GC layer first: its directory
comes from `maintaining-host-disk` ([`DISK_GC.md`](DISK_GC.md)).
