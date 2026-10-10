# Backup and recovery (#185)

The nightly backup and the runbook around it. The pattern is the cohort's —
CannObserv/broker#4, watcher#296 D8/D9, watcher#297 — ported from watcher's
code; where this repo departs, the reason is stated. Dated drill results go in
[`RECOVERY-REHEARSALS.md`](RECOVERY-REHEARSALS.md).

## What is backed up

| | Database | `./data/` archive |
|---|---|---|
| **What** | `pg_dump --format=custom` of `wslcb` (33.4 MB, 4.8 s on 2026-09-29) | Every file under `wslcb/licensinginfo/`, `wslcb/licensinginfo-diffs/`, `wslcb/licensinginfo-internet_archive/`, `remediation-backups/` |
| **Not** | roles (cluster state — `wslcb` and `wslcb_backup` are recreated by hand) | `licensinginfo-replay/` (derived: `wslcb ingest generate-replay-extracts`), `data/*.md` |
| **Bucket** | `gs://co-gcs-wslcb-backup` | `gs://co-gcs-wslcb-archive` |
| **Key** | `daily/<host>/<stamp>.dump`; also `monthly/<host>/<stamp>.dump` on the month's first run | `<host>/<path under data/>` |
| **Retention** | lifecycle: `daily/` 30 days, `monthly/` 365 days | **none — never deleted** |
| **Verified** | `pg_restore --list` finds `alembic_version`, `license_records`, `sources`, `record_sources`; `pg_restore -f /dev/null` reads it through; sha256 in metadata | md5 vs. the bucket's `md5Hash`; a local file that differs from its object is a **failure**, never an overwrite |
| **RPO** | 24 h (scraped records since the dump replay from the archive — see *Restore*) | 24 h, plus a 10-minute settle window for files still being written |

`<host>` is `hostname` (`wslcb-licensing-tracker`) unless `WSLCB_BACKUP_PREFIX`
says otherwise. Both phases run every night, `08:17 America/Los_Angeles` ±10 min
(after the 06:30 PT scrape has finished), as `wslcb-backup.service`
(`infra/`). Either failing fails the unit and sends an `alert` check-in.

**Why the departures.** *Monthlies* — the cohort keeps a flat 30 days, but
damage to frozen data has gone unnoticed here for weeks (#151/#152); a flat
window could leave only damaged dumps. The tier leads the key so each
lifecycle rule is a plain `matchesPrefix`. *The archive* — no cohort service
backs up files. It reuses the dump's create-only SDK path rather than restic,
whose GCS backend deletes its own lock files and whose `forget --prune` deletes
data: both need the `delete` the writer must never hold. The pages are public,
so restic's encryption buys nothing. A separate bucket, so no lifecycle edit on
the dump bucket can ever reach it. *`pg_dump --file`* rather than watcher's
stdout fd: through stdout `pg_dump` appended a second table of contents, and a
cut removing only that copy passed both verification reads (measured
2026-10-09).

**When the archive reports `differs`.** A file under `data/` no longer matches
the object it was first shipped as. These files are frozen, so one side is
damaged: compare the local file with `restore-archive --path <it>`. If the local
copy is the bad one, put the archived bytes back. The alert repeats nightly
until the two agree; nothing is ever shipped over the object.

**Why create-only.** The service account holds `objectCreator` + `objectViewer`
on both buckets and nothing else: no overwrite, no delete. A compromised host
cannot erase its history; retention belongs to the bucket. GCS soft-delete
(7 days, the default) stays on as a net under an administrator's mistake.

## Step 0 — what no backup holds

A rebuilt host needs these from their owners, not from GCS:

- `/etc/wslcb-licensing-tracker/.env`: `DATABASE_URL` (the `wslcb` role's
  password, reset on the new cluster), `ADDRESS_VALIDATOR_API_KEY` (from
  CannObserv/address-validator's owner), `ENABLE_ADDRESS_VALIDATION`.
- The backup's own three files (below) — a fresh key from the GCP owner, the
  check-in key from co-status.
- The repo-root `.env` (GitHub PATs, `TEST_DATABASE_URL`) and the deploy key.
- A tailnet auth key (below).

Code is on GitHub; `infra/` holds every unit.

## Provisioning — needs the GCP project owner

The node has no `gcloud` and no identity that can create or read bucket
config. Run from a workstation with `roles/storage.admin` on `co-gcs`:

```bash
PROJECT=co-gcs
SA=co-wslcb-backup
MEMBER="serviceAccount:$SA@$PROJECT.iam.gserviceaccount.com"
LOCATION=$(gcloud storage buckets describe gs://co-gcs-blobs --format="value(location)")

for BUCKET in co-gcs-wslcb-backup co-gcs-wslcb-archive; do
    gcloud storage buckets create "gs://$BUCKET" --project="$PROJECT" --location="$LOCATION" \
        --uniform-bucket-level-access --public-access-prevention
done

# Dumps only. The archive bucket gets NO lifecycle rule, ever.
cat > /tmp/wslcb-lifecycle.json <<'EOF'
{"rule": [
  {"action": {"type": "Delete"}, "condition": {"age": 30,  "matchesPrefix": ["daily/"]}},
  {"action": {"type": "Delete"}, "condition": {"age": 365, "matchesPrefix": ["monthly/"]}}
]}
EOF
gcloud storage buckets update gs://co-gcs-wslcb-backup --lifecycle-file=/tmp/wslcb-lifecycle.json
for BUCKET in co-gcs-wslcb-backup co-gcs-wslcb-archive; do
    gcloud storage buckets describe "gs://$BUCKET" \
        --format="yaml(name, location, lifecycle_config, soft_delete_policy, versioning_enabled)"
done
#   gcloud storage's key names; the API's camelCase spellings print nothing rather than erroring

gcloud iam service-accounts create "$SA" --project="$PROJECT" \
    --display-name="wslcb-licensing-tracker DB + data backup writer"
for BUCKET in co-gcs-wslcb-backup co-gcs-wslcb-archive; do
    for ROLE in roles/storage.objectCreator roles/storage.objectViewer; do
        gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member="$MEMBER" --role="$ROLE"
    done
done
gcloud iam service-accounts keys create co-wslcb-backup.json --iam-account="${MEMBER#serviceAccount:}"
```

The `describe` must show the two rules on the backup bucket and **none** on the
archive. Copy `co-wslcb-backup.json` to the node by `scp`, then delete the
workstation copy.

## Tailnet

The check-in goes to co-status at `http://status:9000`, reachable only over
the tailnet, as for watcher (its `docs/reference/tailscale.md` is the
reference). Policy first, from the admin console: a `tag:wslcb` whose only
grant is `tag:wslcb → tag:status:9000`, and **no** `tag:wslcb` source in any
`ssh` or `:22` rule. Then mint a key that is `tag:wslcb`-only, pre-approved and
**not ephemeral** (an ephemeral node vanishes on a clean shutdown and takes its
grants with it), and on the node:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
read -rsp 'tailnet auth key: ' KEY; echo
sudo tailscale up --auth-key="$KEY" --hostname=wslcb --advertise-tags=tag:wslcb; unset KEY
tailscale status | head -3                    # shows tag:wslcb
curl -s http://status:9000/health             # answers, "environment": "production"
```

The unit orders `After=tailscaled.service` without `Wants=`: a tailscale
restart must not take a backup down, and a check-in that fails is itself what
the monitor alarms on.

## The co-status monitor

Created on co-status, not here (CannObserv/status#30, 2026-10-10): monitor
`co-wslcb-backup`, id `01M4KCE70FJSJKCP2WFH990CAA`, tenant `co-wslcb`, on the
same two notifier channels as `co-usa-wa-backup` and `co-observo-backup`. A
rebuilt host keeps the id and needs only the key, re-issued by co-status's
operator, terminal to terminal. The fields:

| Field | Value | Why |
|---|---|---|
| `interval_seconds` | `86400` | one run a day |
| `grace_seconds` | `7200` | the timer's 10-minute jitter plus the unit's one-hour timeout — a run killed by it cannot check in (`tests/test_infra_backup_units.py` holds them to this) |
| `renotify_seconds` | `86400` | repeat daily while missing |
| `title_template` / `body_template` | `wslcb backup {{ outcome }} on {{ source_host }}` / `{{ error }}` | rendered for an `alert` only |
| `enabled` | `true`, stated | a disabled monitor still delivers `alert`s but never alarms on silence — it looks wired and is not |

The check-in is `POST /api/v1/monitors/<id>/checkin`, `X-API-Key` (not
`Bearer` — that is a 403), body `{"status": "ok"|"alert", "variables": {…}}`.
An `alert` carries `source_host`, `outcome` (`failed`) and `error`; an `ok`
carries `outcome`, `source_host`, `db_outcome`, `object`, `monthly`,
`dumped_at`, `size_bytes`, `sha256`, `alembic_head` and
`archive_{uploaded,unchanged,unsettled,failed}`. Use the monitor's id, not its
`tenant_id` — both are ULIDs, and the wrong one is a 404.

## Install and first run

**The role first**, once per cluster (purely additive; read its header):

```bash
sudo -u postgres psql -d wslcb < scripts/setup-backup-role.sql
```

Its report must read `t` for `login`, `inherit`, `no_password`,
`reads_all_data`, `can_connect`, `f` elsewhere, and `0` for `unreadable`,
`under_row_security`, `large_objects`.

**Then the venv** must hold `google-cloud-storage`: `uv sync` in the checkout
after the merge. **Then three files**, the keys `0400 root:root`, read only by
systemd; the check-in key **empty** until the monitor exists, because a missing
`LoadCredential=` source fails the start:

```bash
sudo install -m 0400 -o root -g root co-wslcb-backup.json /etc/wslcb-licensing-tracker/co-wslcb-backup.json
shred -u co-wslcb-backup.json
sudo install -m 0400 -o root -g root /dev/null /etc/wslcb-licensing-tracker/backup-checkin.key
sudo tee /etc/wslcb-licensing-tracker/backup.env >/dev/null <<'EOF'
WSLCB_BACKUP_BUCKET=co-gcs-wslcb-backup
WSLCB_ARCHIVE_BUCKET=co-gcs-wslcb-archive
EOF
sudo chmod 0644 /etc/wslcb-licensing-tracker/backup.env
```

**No `GOOGLE_APPLICATION_CREDENTIALS` in `backup.env`**: the unit points it at
the run's private copy (`%d/gcs`), and an env file's value would win, aiming
the job at the root-only original. The job refuses that — exit 2 and an
`alert` naming the file — rather than fail `Permission denied`, which reads as
a reason to loosen the key's mode.

```bash
sudo cp infra/wslcb-backup.service infra/wslcb-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start wslcb-backup.service; journalctl -u wslcb-backup -n 20 -o cat --no-pager
sudo systemctl enable --now wslcb-backup.timer          # only once the run above succeeded
```

The first run uploads the whole archive (~350 MB, ~4,900 files); a run cut
short by the one-hour timeout resumes where it stopped. Note the journal's
`Consumed … memory peak` line in the rehearsal log (#175: no swap here).
`243/CREDENTIALS` is a key file missing; `203/EXEC` an interpreter the empty
`/home` hides; `FATAL: role "wslcb_backup" does not exist` is the role script
not yet run.

**Then the check-in**, once the monitor exists — the key read at a prompt so it
reaches neither history nor argv, and through `tee` so the file keeps its mode:

```bash
read -rsp 'check-in key: ' KEY; echo
printf '%s' "$KEY" | sudo tee /etc/wslcb-licensing-tracker/backup-checkin.key >/dev/null; unset KEY
sudo tee -a /etc/wslcb-licensing-tracker/backup.env >/dev/null <<'EOF'
WSLCB_BACKUP_CHECKIN_BASE_URL=http://status:9000
WSLCB_BACKUP_MONITOR_ID=<monitor id>
EOF
sudo systemctl start wslcb-backup.service
```

All three or none: half a configuration logs an ERROR naming what is missing
and checks in nothing. A landed check-in shows in the journal only as httpx's
`"HTTP/1.1 202 Accepted"`; the proof is the monitor's fresh last check-in.
**Then see the alarm fire** before relying on it: narrow the monitor to
`interval_seconds` 60 / `grace_seconds` 0, wait for *"has stopped reporting"*,
start the unit for *"has recovered"*, and restore 86400 / 7200.

### Prove the grant is create-only

By observation, on a probe object of its own, in **each** bucket: create it,
then try to overwrite and to delete it. Both must answer **403**. (An
`if_generation_match=0` upload over an existing name proves nothing: GCS
answers 412 whatever the grant.) As root, the one identity that can read the key:

```bash
sudo bash -c "GOOGLE_APPLICATION_CREDENTIALS=/etc/wslcb-licensing-tracker/co-wslcb-backup.json .venv/bin/python -" <<'PY'
from datetime import UTC, datetime
from google.api_core.exceptions import Forbidden
from google.cloud import storage
for name in ("co-gcs-wslcb-backup", "co-gcs-wslcb-archive"):
    blob = storage.Client().bucket(name).blob(f"probe/{datetime.now(UTC):%Y%m%dT%H%M%SZ}")
    blob.upload_from_string(b"probe", if_generation_match=0)  # the create: must succeed
    for attempt, act in (("overwrite", lambda: blob.upload_from_string(b"again")),
                         ("delete", blob.delete)):
        try:
            act()
            print(f"{name} {attempt}: ALLOWED — the grant is too wide")
        except Forbidden:
            print(f"{name} {attempt}: 403 — create-only holds")
PY
```

The probes sit under `probe/`, outside every listing the tools read; being
undeletable by design, they stay (five bytes each).

## Restore

Run as root with the key, from the checkout (`sudo -i`, then):

```bash
cd /home/exedev/wslcb-licensing-tracker
export GOOGLE_APPLICATION_CREDENTIALS=/etc/wslcb-licensing-tracker/co-wslcb-backup.json
export WSLCB_BACKUP_BUCKET=co-gcs-wslcb-backup WSLCB_ARCHIVE_BUCKET=co-gcs-wslcb-archive
.venv/bin/wslcb ops restore --list                           # every host's dumps, both tiers
```

**Name the source host.** `--latest` requires `--prefix HOST` and never
defaults to this host: once a rebuilt host's own timer has run, its newest dump
is a real, verifiable dump of the wrong database. `--latest` takes the newest
by stamp across both tiers (monthlies serve once the dailies have aged out) and
passes over any name later than the bucket's own creation time for it (a skewed
clock or a forgery; `--list` marks them `SUSPECT`).

### The database

On a fresh cluster, the owning role and an empty database first — the dump
carries ownership and grants, not roles:

```bash
sudo -u postgres createuser --login wslcb && sudo -u postgres psql -c "\password wslcb"
sudo -u postgres createdb -O wslcb wslcb
.venv/bin/wslcb ops restore --latest --prefix wslcb-licensing-tracker --into wslcb --run-as postgres
sudo -u postgres psql -d wslcb < scripts/setup-backup-role.sql
```

`--into` loads with `--exit-on-error --single-transaction`, on stdin, so a
failure leaves the target empty and `postgres` never reads a root-written
file. Fetch-only, to inspect: `--download-only DIR` (a new or private `0700`
directory; the file is written `O_EXCL`, `0600`, sha256- and
`pg_restore`-verified).

**Go/no-go**, before starting `wslcb-web`:

1. `psql -d wslcb -Atc "SELECT version_num FROM alembic_version"` equals the
   dump's `alembic_head` (`--list` shows it).
2. Row counts match the dump's own: `pg_restore --data-only -f - <dump> | awk
   '/^COPY /{t=$2;n=0;next} /^\\\.$/{print t, n; next} {n++}'` against
   `SELECT count(*)` per table.
3. `wslcb db check` is clean.

**Then recover what came after the dump.** Restore the archive (below) first,
then `wslcb ingest backfill-snapshots` and `wslcb ingest backfill-diffs`
re-ingest every page scraped since — idempotent, duplicates are skipped — so
scraped records are lost only back to the archive's last run. Address
validations since the dump are lost; `backfill-addresses` re-runs them at the
usual quota cost.

### The archive

```bash
.venv/bin/wslcb ops restore-archive --prefix wslcb-licensing-tracker --into /root/data-restore [--path wslcb/licensinginfo/2026]
```

Files land at their `data/`-relative paths under a private directory, `0600`,
md5-checked, never over an existing file. Move them into `data/` as `exedev`
(`chown -R exedev:exedev`, `0644`/`0755`), never over a live file: the archive
is only ever *added* to.

## Drill

Quarterly, and after any change to `backup.py`, `restore.py` or the unit. Into
a scratch database, never `wslcb`:

```bash
sudo -u postgres createdb -O wslcb wslcb_drill
.venv/bin/wslcb ops restore --latest --prefix wslcb-licensing-tracker --into wslcb_drill --run-as postgres
DATABASE_URL="$(sed -n 's|^DATABASE_URL=\(.*\)/wslcb$|\1/wslcb_drill|p' /etc/wslcb-licensing-tracker/.env)" \
    .venv/bin/wslcb db check
.venv/bin/wslcb ops restore-archive --prefix wslcb-licensing-tracker --into /root/drill-data --path remediation-backups
sudo -u postgres dropdb wslcb_drill; rm -r /root/drill-data
```

plus go/no-go 1 and 2 above against `wslcb_drill`. Record the date, object,
timings and anything that surprised you in `RECOVERY-REHEARSALS.md`. Every test
run also rehearses a real `pg_dump` → fake bucket → `pg_restore` round trip
(`tests/test_backup_restore_rehearsal.py`; needs `TEST_DATABASE_URL`).
