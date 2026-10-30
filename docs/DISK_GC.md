# Disk GC (#200)

Tooling garbage on this host is the cohort's disk GC's, not the repo's: Provisioner's
GC and headroom sensor (CannObserv/provisioner#61), pulled from a digest-pinned
`co-image-base` release and installed by the vendored `maintaining-host-disk` skill
(its `scripts/_pin.sh` pins the `VERSION`; an update is a `skills-vendor` bump, then the
skill's pull and install). Its files live under `/usr/local/lib` and are never edited
here: raise a rule change through the operator. It replaced this repo's own tooling
tiers in #200. What stays repo-side: the data stragglers
(`wslcb-compress-stragglers.timer`), pytest's basetemp knobs (`pyproject.toml`), and
the journal's `Storage=persistent` ([`DEPLOYMENT.md`](DEPLOYMENT.md) § Journal).

This host's `/etc/cannobserv/disk-gc/disk-gc.conf` (root, 0644), from the template:

| Key | Value | Why |
|---|---|---|
| `homes` | `/home/exedev` | the only user with tooling trees |
| `tiers` | the template's, plus `worktrees` | `docker` stays off: Docker runs only SocratiCode's containers, and the GC never removes a container or volume |
| `npx_spec_files` | `/home/exedev/wslcb-licensing-tracker/.claude/settings.json` | keeps the pinned SocratiCode tree; the GC reads VS Code's machine setting (`claudeCode.environmentVariables`, the one that delivers the pin, #180) itself |
| `worktree_repos` | `/home/exedev/wslcb-licensing-tracker` | covers `.worktrees/` and `.claude/worktrees/` |
| `alert_at_percent` / `clear_at_percent` | 80 / 75 | baseline after a GC run plus a staged editor build and a worktree; patching-hosts refuses a step that would cross 80 |
| `monitor_id` | `co-wslcb-disk-headroom`'s id | tenant `co-wslcb`; `checkin.key` (root, 0600) is the tenant key, as `backup-checkin.key` |

The GC runs daily at 05:15 UTC (+≤30 min, no catch-up), the sensor every 10 minutes.
VERSION 3's `vscode-servers` keeps every server build newer than the newest live one,
so staged builds stay until the client moves to them.

```bash
sudo /usr/bin/python3 -I /usr/local/lib/cannobserv/disk-gc/disk-gc gc   # report only, removes nothing
sudo journalctl -u cannobserv-disk-gc -u cannobserv-disk-headroom --since today
sudo journalctl -p err -u cannobserv-disk-gc                            # sudo: else "-- No entries --"
```
