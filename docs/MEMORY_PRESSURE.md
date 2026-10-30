# Memory pressure (#175)

This VM has **7.2 GiB and no swap**, and the production service shares it with
interactive agent sessions. That combination has a specific failure mode, seen
on a sibling host on 2026-09-16: nothing gets OOM-killed. exe.dev session
processes inherit `oom_score_adj` **-1000** from exe.dev's sshd
(`/exe.dev/bin/sshd`, itself running at -1000 under PID 1), so the
kernel killer can never pick the session or anything it launches — under real
exhaustion it takes the production service instead, while the kernel fails
*atomic* allocations in unrelated processes (`tailscaled`, `ksoftirqd`). A
cgroup cap on a process the killer won't touch **stalls** it rather than
killing it, so the reservation has to go on the service, not the session.

earlyoom does not change that (#178): v1.7 skips -1000 processes just as the
kernel does, so it can never take the session's `node`/`npm` either.
Its `--prefer` therefore names SocratiCode's containers (`llama-server`,
`ollama`, `qdrant`: adj 0, the largest transient RSS, restarted on demand), and
its `--avoid` adds `postgres`. Without that, a Postgres backend ranked next
after the containers, and SIGKILL on any backend resets every connection.

Three independent pieces, none of which substitutes for another:

| Piece | File | Applies |
|---|---|---|
| Service reservation — `MemoryLow=256M`, `OOMScoreAdjust=-700` | `infra/wslcb-web.service` | `sudo cp` + `daemon-reload` + restart (see [After changing service files](DEPLOYMENT.md#after-changing-service-files)) |
| **Parent slice grant — `MemoryLow=512M`** (without it the row above is inert) | `infra/system.slice.d-10-wslcb-memory.conf` | `sudo install -D -m 644 infra/system.slice.d-10-wslcb-memory.conf /etc/systemd/system/system.slice.d/10-wslcb-memory.conf && sudo systemctl daemon-reload` |
| Kernel atomic-allocation reserve — `vm.min_free_kbytes=65536` | `infra/sysctl.d-60-wslcb-memory.conf` | `sudo install -m 644 infra/sysctl.d-60-wslcb-memory.conf /etc/sysctl.d/60-wslcb-memory.conf && sudo sysctl --system` |
| Userspace OOM killer, acts before the kernel | `infra/default-earlyoom` | `sudo apt install earlyoom && sudo install -m 644 infra/default-earlyoom /etc/default/earlyoom && sudo systemctl enable --now earlyoom && sudo systemctl restart earlyoom` — the **restart is required**, see below — then `journalctl -u earlyoom -n 12 --no-pager` to confirm the parsed thresholds and both regexes |

One command checks the whole stack against what `infra/` declares — effective
cgroup protection, `oom_score_adj`, `vm.min_free_kbytes`, earlyoom's *parsed*
args and its live top-3 victims (it fails if the top one is `--avoid`ed),
that both SocratiCode launches name one pinned version, and that SocratiCode's
containers publish on loopback only with Qdrant telemetry off (#222):

```bash
scripts/verify-memory-pressure.sh          # exit 0 ok · 1 drift · 2 couldn't check
```

It reads every expectation from `infra/` — and the session pin from
`.claude/settings.json` and VS Code's machine settings — rather than hardcoding them, so it
cannot drift from the committed config; run it after any change here and after
a reboot. `tests/test_infra_memory_pressure.py` is its counterpart — that
checks the config files are coherent with each other, this checks the running
host matches them. The rest of this section is what to do when it reports drift.

`MemoryLow=` does not work alone. cgroup v2 limits a unit's effective low
protection by *every* ancestor's, and `system.slice` ships with `memory.low=0`
— so `min(256M, 0) = 0`. Normally the `memory_recursiveprot` mount option makes
protection propagate without per-level config, and systemd ≥ 247 sets it when
it mounts the hierarchy. Here the mount is bare `rw` — neither
`memory_recursiveprot` nor `nsdelegate`, systemd's other default — so systemd
did not perform it. **What did is undetermined**: PID 1 is systemd and no
`exe-init` process exists on this host. The cause is open; the effect is
measured. Verify before trusting the reservation:

```bash
grep cgroup2 /proc/self/mountinfo               # memory_recursiveprot present?
cat /sys/fs/cgroup/system.slice/memory.low      # must be >= the child's grant
```

Both readings are why the slice drop-in is a required row above, not an
optional hardening step.

`OOMScoreAdjust=-700` is **calibrated, not arbitrary**: earlyoom 1.7 adds 300
to a `--prefer` match's score, so the web service ranks below one only if it
sits under 300. It also ranks below every killable adj-0 process. Measured on
this host: adj 0 → ~674, -500 → 341 (still loses), -700 → ~208. Re-measure
with systemd's own view of the main PID:

```bash
cat /proc/$(systemctl show -p MainPID --value wslcb-web.service)/oom_score
```

Do **not** reach for `pgrep -f 'uvicorn wslcb'` here: `-f` matches against full
command lines, so any shell whose own command contains that string matches
itself first and reports `-1000`/`0` (the session's inherited values) — which
reads exactly like a correctly-protected service. That misfire happened during
this rollout. Re-measure
if either the service's footprint or the earlyoom config changes — the two are
coupled, and changing one alone silently re-arms the bug.

## The journal is off tmpfs (#186)

Until #186, the journal lived in `/run/log/journal`. `/run` is tmpfs, so on a
host with no swap the journal held RAM: up to its 147.6M runtime cap before it
rotated. It's on disk now, capped at 512M
([DEPLOYMENT.md § Journal](DEPLOYMENT.md#journal-186)). If `/run/log/journal`
grows again, the flush has been re-masked.

## Verifying earlyoom actually took the config

`apt install earlyoom` **starts the daemon itself**, before any config is in
place. `systemctl enable --now` is then a no-op on an already-active unit, so
the daemon keeps running with Debian's stock `EARLYOOM_ARGS="-r 3600"` — no
`--prefer`, no `--avoid`, and default 10%/5% thresholds. It reports `active`
and `enabled` throughout, which is why this needs checking rather than
assuming. An explicit `systemctl restart earlyoom` is what applies the config.

The journal is the only place the daemon says what it actually parsed. All
four lines must be present:

```
Preferring to kill process names that match regex '^(node|npm|npx|llama-server|ollama|qdrant)'
Will avoid killing process names that match regex '^(uvicorn|sshd|systemd|exe-init|postgres|.sd-pam)'
sending SIGTERM when mem <=  6.00% and swap <= 10.00%,
        SIGKILL when mem <=  3.00% and swap <=  5.00%
```

Two missing regex lines and 10.00%/5.00% thresholds mean the stock config is
still loaded. Confirmed working on this host with earlyoom v1.7 on 2026-09-19.

## Don't let a tool install at launch

The measured peak on this class of host was never the workload — it was the
*install*. A cold `npx -y --prefer-online socraticode@latest` reached 1.2 G at
the cgroup, with all 126 `MemoryHigh` throttle events in the install and none
in the indexing that followed; the same work from a pinned, pre-installed entry
peaked at 75 MB. `--prefer-online` revalidates against the registry on every
launch, so a warm cache is not a warm path.

SocratiCode is therefore **pinned** on this host — one deliberate, capped
install per version instead of one per launch. It has two launches, each with
its own pin, and both name **1.14.0**:

- **Driver** (`mcp-driver.mjs` — the health hook, `index`, `verify`): a
  pre-install at `~/.socraticode/pin`, outside the repo and inert when absent,
  so a fresh clone's driver resolves exactly as before.
- **Plugin session** (the server Claude Code starts): the plugin launches
  `npx -y --prefer-online ${SOCRATICODE_SPEC:-socraticode@latest}`, and Claude
  Code expands that from **its own startup environment**. The `env` block in
  `.claude/settings.json` is merged only into the processes it launches — the
  server received the variable and still ran `@latest` (#180,
  gregoryfoster/skills#332). So the session pin has two halves:
  - *declared* as `SOCRATICODE_SPEC` in the committed `.claude/settings.json`
    — the value the tests and the verifier hold everything else to;
  - *delivered* by VS Code's `claudeCode.environmentVariables` in
    `~/.vscode-server/data/Machine/settings.json`. The key is machine-scoped,
    so it cannot be a committed workspace setting, and it reaches only a
    `claude` started after a full reconnect of the remote. From a terminal,
    export the variable in the shell that launches `claude`.

Re-pinning changes all of them, to one version:

```bash
npm view socraticode version        # pick a literal; never @latest
CAP=(-p MemoryHigh=1200M -p MemoryMax=1536M -p CPUQuota=100%)
systemd-run --user --scope "${CAP[@]}" choom -n 500 -- npm install --prefix ~/.socraticode/pin socraticode@<version>
systemd-run --user --scope "${CAP[@]}" choom -n 500 -- npm exec -y --package=socraticode@<version> -- true   # warms the session's npx cache
# then write socraticode@<version> into .claude/settings.json's SOCRATICODE_SPEC,
# the SOCRATICODE_SPEC entry of claudeCode.environmentVariables in
# ~/.vscode-server/data/Machine/settings.json, and the version this section
# states above — tests/test_infra_memory_pressure.py holds the three together
node skills-vendor/gregoryfoster-skills/skills/init-socraticode/scripts/mcp-driver.mjs resolve
# then fully reconnect the VS Code remote, so claude restarts with the new value
```

The cohort disk GC's `npx` tier (#200, [`DEPLOYMENT.md`](DEPLOYMENT.md) § Disk GC)
never prunes the plugin session's npx tree, `~/.npm/_npx/<sha512(spec)[:16]>`. It
reads the spec from both halves of the session pin: the declared one through its
`npx_spec_files` (this repo's `.claude/settings.json`), and the delivered one from VS
Code's machine setting, which it reads itself. Its report lists the tree as `pinned
<spec>`. If either source exists but can't be parsed, it keeps every npx tree and
warns. (`~/.socraticode/pin` is outside `_npx`, so the GC never touches it.)

After a re-pin, the old version's tree goes on the GC's next daily run once nothing
uses it (`/proc`) and no entry in it is newer than `grace_minutes` (60). There is no
age gate: #217's 30 days retired with this repo's own tier.

`choom -n 500` makes each install killable: without it the scope inherits the
session's -1000 and the cap stalls it instead (see above). `resolve` prints
which path won without starting a server; it should report
`pinned install v<version>`, not `npx`.

For the session, the only evidence is the server that the session's `claude`
launched. Don't trust a manifest (the plugin ships three, and two still
hardcode `@latest`). Don't trust `claude mcp list` from a session shell either:
that shell already carries the variable, so it reports what a launch *with* it
would do. preflight reads the process table instead (gregoryfoster/skills#332):
only its *"Plugin session launched … — observed: …"* pass is evidence, and a
*"… — not observed: …"* warning reports the declared pin, not a launch — check
by hand:

```bash
ps -eo pid,ppid,args | grep '[n]pm exec socraticode'   # expect socraticode@<version>, ppid = the extension's claude
```

`scripts/verify-memory-pressure.sh` checks every link: the declared value
against the driver's pin, the machine setting against the declared value, and
the running session's server against both. It fails on any disagreement.
`.claude/hooks/socraticode-health.sh` measures drift only
against a *floating* session spec, so once `SOCRATICODE_SPEC` is a literal it
stays silent, even for a literal that differs from the driver's.

## SocratiCode's containers: loopback only, telemetry off (#222)

Docker publishes a container's ports ahead of the host firewall, and this host
is on the tailnet, so a port published on every interface reaches every peer.
Qdrant's API has no authentication. SocratiCode starts an existing
`socraticode-qdrant` or `socraticode-ollama` by name and keeps whatever
bindings it was created with. Only when no container of that name exists does
it create one, and then it publishes on every interface (`-p 16333:6333`, with
no host IP and no setting to add one), with Qdrant's anonymous telemetry on.
Both containers were therefore recreated by hand on 2026-10-26: published on
`127.0.0.1` only, Qdrant with `QDRANT__TELEMETRY_DISABLED=true`. That survives
SocratiCode restarts and upgrades, `docker restart` and reboots
(`unless-stopped`). It is lost only if a container is removed.

To replace a wide container, or to recreate a lost one before the next session
does, run the commands below. The index lives in the named volumes, so it
survives. Rename rather than remove: SocratiCode's existence check is a
substring match on `docker ps -a --filter name=…`, so while `…-old` exists no
session can create a wide container in the gap, and `…-old` is the rollback.
For a lost container, skip the `stop`/`rename` line. Use the image tag the
pinned SocratiCode names in `dist/constants.js` (`QDRANT_IMAGE`,
`OLLAMA_IMAGE`). An upgrade that changes it pulls the new image but keeps
starting the old container.

```bash
docker stop socraticode-qdrant && docker rename socraticode-qdrant socraticode-qdrant-old
docker run -d --name socraticode-qdrant -p 127.0.0.1:16333:6333 -p 127.0.0.1:16334:6334 \
  -v socraticode_qdrant_data:/qdrant/storage --restart unless-stopped \
  -e QDRANT__TELEMETRY_DISABLED=true qdrant/qdrant:v1.17.0
docker stop socraticode-ollama && docker rename socraticode-ollama socraticode-ollama-old
docker run -d --name socraticode-ollama -p 127.0.0.1:11435:11434 \
  -v socraticode_ollama_data:/root/.ollama --restart unless-stopped ollama/ollama:latest
```

Capture the point counts before and compare them after. They must match:

```bash
for c in $(curl -s localhost:16333/collections | jq -r '.result.collections[].name' | sort); do
  echo "$c $(curl -s -X POST localhost:16333/collections/$c/points/count \
    -H 'content-type: application/json' -d '{"exact":true}' | jq .result.count)"
done
```

`ollama/ollama:latest` resolves to whatever image is tagged locally. If that
tag is gone, `docker run` pulls today's `latest`, a different embedder than the
one that wrote the stored vectors (0.32.5 as of #222). Before the recreate,
note `curl -s localhost:11435/api/version` (or
`docker image inspect -f '{{.Id}}' ollama/ollama:latest`). If it differs after,
re-index with `codebase_index` rather than trust the old vectors.

Then `ss -ltn` must list 16333, 16334 and 11435 on `127.0.0.1` only, and
`docker logs socraticode-qdrant 2>&1 | grep -i telemetry` must end with
`Telemetry reporting disabled`. Run a `codebase_search`, then
`docker rm socraticode-qdrant-old socraticode-ollama-old`. To roll back, run
`docker rm -f <name> && docker rename <name>-old <name> && docker start <name>`.

`scripts/verify-memory-pressure.sh` (section 6) fails if a container is
missing, if any published host IP or live listener is not loopback, or if
Qdrant's telemetry is not off. Run it after anything that removes containers.
