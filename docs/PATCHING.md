# Patching

OS package updates on this host: the scheduled `patching-hosts` run and the
third-party apt origins it follows. Services and the copy block:
[`DEPLOYMENT.md`](DEPLOYMENT.md).

## OS security updates

The exeuntu image masks the apt timers, so patching is a scheduled monthly run
under the vendored `patching-hosts` skill (#192; first run by hand in #184,
gregoryfoster/skills#313). Its host knob, `.skills/patching-hosts`, declares the
window, a UTC `quiet` range per timer firing (derived from `infra/*.timer` by
`tests/test_patching_hosts_knob.py`, #223), the in-flight gate, the restarter
and the Postgres databases; see [`SKILLS.md`](SKILLS.md#patching-hosts). Its
`backup` line makes a fresh `wslcb-backup.service` run (#185) the recovery
point for `wslcb` (#237), in place of a second `pg_dump`. Its two third-party
origins are followed, each in its own held step and scoped by pins: Tailscale
(`origin pkgs.tailscale.com follow`, `infra/preferences.d-tailscale`,
[`RECOVERY.md`](RECOVERY.md#tailnet), #220) and NodeSource
(`origin deb.nodesource.com follow`, [Node.js](#nodejs) below, #239).
apt's needrestart hook (`/etc/apt/apt.conf.d/99needrestart`)
runs `/usr/lib/needrestart/apt-pinvoke -m u … || true`, which calls needrestart;
Ubuntu turns that into *automatic* restarts unless `$nrconf{restart}` is set.
Postgres, `wslcb-web` and dockerd all map libc6/libssl3t64, and a Postgres
restart 503s `/api/v1/health`, which `wslcb-healthcheck` answers with its own
`systemctl restart wslcb-web`. `infra/needrestart.conf.d/wslcb.conf` sets
`'l'`, so an apply only lists what needs restarting; restarts happen in a
window, under the owner's approval.

```bash
sudo install -m 644 infra/needrestart.conf.d/wslcb.conf /etc/needrestart/conf.d/
sudo needrestart -m u -b -r l   # must print "Disabling Ubuntu mode, explicit restart mode configured"
```

- That line proves the *file's* key loaded; `-r l` keeps the proof itself from
  restarting anything. Never run the proof without it.
- Belt and braces on every apply: `sudo NEEDRESTART_MODE=l choom -n 0 -- unattended-upgrade -v`
  (`choom` because the session inherits adj -1000 from exe.dev's sshd).
- Maintainer scripts are out of its reach: `postgresql-16` restarts the cluster
  and `containerd` may restart itself. Hold them as separate steps, with
  `wslcb-healthcheck.timer` stopped across the Postgres one.
- `tests/test_needrestart_config.py` evaluates the file as Perl, and on the
  host pins the installed copy and the live `conf.d` chain.
- **After a run,** `systemd-analyze get-log-target` reads `journal` right
  after a boot (exe-init's argv) and `journal-or-kmsg` after any reload or
  re-exec ([PID 1's log target](LOGGING.md#pid-1s-log-target)); `systemd`'s re-exec
  keeps it (measured on CannObserv/provisioner#67). Both are expected.
  `console` is a finding.

### Node.js

Node runs SocratiCode's MCP server and the JS test harness (`npm run test:js`),
never a service: no `infra/` unit runs it, and the web process loads none. It
comes from NodeSource's `node_26.x` apt repo (Node 26, end of life 2029-04-30),
which replaced universe's end-of-life `nodejs` 18 and Ubuntu's `npm` (#239).

```bash
sudo install -m 644 infra/nodesource-repo.gpg.asc /etc/apt/keyrings/nodesource.asc
sudo install -m 644 infra/sources.list.d-nodesource.sources /etc/apt/sources.list.d/nodesource.sources
sudo install -m 644 infra/preferences.d-nodesource /etc/apt/preferences.d/nodesource
sudo apt-get update
apt-cache policy nodejs | sed -n 1,5p   # Candidate 26.x-1nodesource1; its version row says 600
sudo apt-get install nodejs         # bundles npm and npx
```

That install is for a host without Ubuntu's `npm`. Over it, NodeSource's
`nodejs` conflicts `npm` away but leaves its ~285 orphans; #239's swap purges
the measured set (`prune-plan.sh --take-orphans`) in the same transaction, with
`APT::Get::AutomaticRemove=false`. Never `--autoremove`: it also takes
unrelated orphans.

- Don't run NodeSource's `setup_26.x`: it pins `nodejs` and `nsolid` at 600
  with no catch-all, which the probe reports as `unscoped:deb.nodesource.com`.
  `tests/test_patching_hosts_knob.py` holds the key's fingerprint, the source
  line and the pins.
- **After every `nodejs` step**, the bundled npm must still find SocratiCode's
  pinned tree, `~/.npm/_npx/<sha512(spec)[:16]>`. A changed key re-installs it
  at the next session launch, #175's memory peak. npm 9.2.0 and 11.20.0 derive
  it identically. `--no` refuses an install, so this exits non-zero rather than
  installing. On a miss it still leaves an empty `_npx/<key>` dir; `rmdir` it.
  From the repo root:

  ```bash
  SPEC=$(node -p 'require("./.claude/settings.json").env.SOCRATICODE_SPEC')
  (cd ~ && npm exec --no --package="$SPEC" -- true) && echo pinned tree found
  ```

- Running SocratiCode servers keep the old binary until their session restarts.
- Rollback: remove the three files, `apt-get update`, then
  `sudo apt-get install --allow-downgrades nodejs=18.19.1+dfsg-6ubuntu5 npm`.
  apt never downgrades on its own. That's universe's end-of-life 18, the reason
  for #239.
