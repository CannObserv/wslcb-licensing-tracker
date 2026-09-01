#!/usr/bin/env bash
# managing-skills-doctor: do not remove this marker — install-doctor.sh greps it
# doctor.sh — diagnose and self-heal dangling skill and hook symlinks.
#
# When this repo is vendored via the managing-skills git-submodule + symlink
# pattern, a consumer checkout that hasn't initialized submodules
# (fresh `git worktree add`, shallow CI clone, etc.) leaves the
# .claude/skills/<name> → ../../skills/<name> → ../skills-vendor/.../<name>
# chain dangling. Scripts referenced from SKILL.md then fail with confusing
# "No such file or directory" errors even though the symlinks exist.
#
# This script is installed as a real (non-symlinked) file at
# <repo-root>/.skills/doctor.sh so it remains reachable even when the
# vendor chain is broken. It walks skills/* and .claude/hooks/* symlinks,
# attempts a `git submodule update --init --recursive` if any dangle, and
# prints a clear actionable error if self-healing fails.
#
# A dangling symlink is not the only way the vendor chain breaks, and for a
# long time it was the only one this script could see (issue #185). "Every
# symlink resolves" was standing in for "the submodules are healthy", and the
# two come apart in the state issue #176 was found in: vendored content still
# on disk from an earlier checkout, .git/config carrying no submodule.*
# entries. Every symlink resolves, so the doctor exited 0 before reaching its
# own init and the checkout stayed half-healed indefinitely — one consumer sat
# there for days. So the heal has a second trigger: any skills-vendor/
# submodule `git submodule status` still prefixes with '-'.
#
# .claude/hooks/ is in scope because skill installers link hooks there too
# (issue #99). A dangling skills/<name> surfaces only when that skill is
# invoked; a dangling hook symlink surfaces on every Edit|Write|MultiEdit —
# the highest-frequency tool event there is — as exit 127 naming a path that
# `ls` plainly shows exists. Same failure class, different directory, and one
# heal path covers any future hook a skill installs.
#
# Because the installed copy is a copy, it re-syncs itself from the vendored
# source whenever that source is reachable — see sync_self below.
#
# Designed for use as a Phase 1 preflight in every reviewing-* / shipping-*
# SKILL.md invocation. The doctor runs first so that the resolution loop that
# follows sees a freshly healed symlink chain (issue #63):
#
#   { [ ! -x .skills/doctor.sh ] || bash .skills/doctor.sh; } || exit 1
#   for d in scripts ".claude/skills/$N/scripts" "$HOME/.claude/skills/$N/scripts"; do
#     [ -f "$d/$S" ] && { SD="$d"; break; }
#   done
#   bash "$SD/$S"
#
# Usage: bash .skills/doctor.sh [--check-only] [--verbose] [--no-preflight] [--help]
set -euo pipefail

# Diagnostic stamp only — reported by --version so a bug report can name the
# copy that produced it. Nothing branches on it: sync_self keeps the installed
# copy equal to the vendored source, which makes drift transient and a
# version-comparison mechanism unnecessary.
VERSION="2026-08-31-1"

CHECK_ONLY=0
VERBOSE=0
NO_PREFLIGHT=0
for arg in "$@"; do
  case "$arg" in
    --check-only) CHECK_ONLY=1 ;;
    --verbose|-v) VERBOSE=1 ;;
    --no-preflight) NO_PREFLIGHT=1 ;;
    --version) echo "$VERSION"; exit 0 ;;
    --help|-h)
      cat <<EOF
Usage: bash .skills/doctor.sh [--check-only] [--verbose] [--no-preflight]

Diagnose and self-heal dangling symlinks in skills/ and .claude/hooks/.

If any symlink in those directories does not resolve and a .git directory
is present, runs 'git submodule update --init --recursive' and re-checks.
Exits 0 silently when healthy. Exits non-zero with an actionable error
when self-healing fails or is not possible (e.g. no .git directory).

The same repair also runs when every symlink resolves but a skills-vendor/
submodule is uninitialized — content on disk, nothing registered in
.git/config. Resolving symlinks do not imply healthy submodules, and that
half-healed checkout is the one nothing else detects. A submodule that
.gitmodules holds with 'update = none' is exempt and stays uninitialized
through any repair; one that is not held fails the doctor, in --check-only
because no repair was attempted and after the heal because one was.

.claude/hooks/ is scanned because skill installers link hooks there into
the same vendor chain; a dangling one fails on every file edit rather than
only when a skill is invoked.

Every hook symlink that resolves is also checked for its SessionStart
registration in .claude/settings.json, which is the half that makes it
run at all. The repair printed comes from a one-line <hook>.install
manifest the vendoring skill ships beside the script, so a skill adding a
hook needs no edit here. Advisory by default: it warns and leaves the
exit code alone, because a wiring gap is not a dangling symlink and
Phase 1 preflights gate on that code. Under --check-only the same state
exits 1 — that mode is a deliberate probe, not a review preflight, so a
CI job can finally gate on the wiring (#231). For per-hook gating, loop
install-hook.sh --check instead: it exits 3 per half-installed hook.

Local overrides are regular directories, not part of the symlink chain,
but two things about them are checked anyway (#238). Their per-script
symlinks (skills/<override>/scripts/*) join the dangling scan: they are
how an override tracks the scripts it does not change, and an upstream
rename or deletion strands them while every top-level symlink still
resolves. And each override's SKILL.md frontmatter is compared against
its overrides: target — warning when the recorded version (the vendor
version LAST SYNCED FROM, bumped on every re-sync; or the synced-from:
commit for vendors that ship no version) has fallen behind the vendor
copy. Drift is advisory in every mode including --check-only, and
nothing is ever auto-merged: the point of an override is that upstream
text cannot be applied blindly.

A vendored file committed as a REGULAR FILE where the vendor ships one of
the same name is reported as silently forked (#256) — it stops tracking
upstream forever and nothing else detects it. Only inside a skills/<name>
that is a real directory: a whole-directory symlink cannot fork, and a
declared override is local by definition. The walk covers the skill's own
*.md plus one level under each of scripts/, references/ and assets/ — a
fork nested deeper than that is not seen. pre-ship.sh is exempt by name
(consumers are expected to supply their own), and any other deliberate
divergence is declared one repo-relative path per line in
.skills/forked-ok. Advisory in every mode, and never healed: a fork is
sometimes the right answer, so the operator decides.

Re-syncs .skills/doctor.sh from the vendored source under skills-vendor/
when the two differ, so upstream fixes reach consumers that did not
install the auto-refresh hook. Best-effort — never affects the exit code;
failures are reported only under --verbose. The refresh applies from the
following run. Skipped entirely under --check-only, which makes no writes.

When submodule init fails with a well-known SSH/HTTPS auth signature
(Permission denied, Could not read from remote repository, Authentication
failed for 'https://'), a targeted remediation block is printed instead
of the generic 'submodule update failed' line. The same block is printed
by the pre-flight SSH check when .gitmodules references SSH remotes and
the agent isn't reachable from this shell. A separate remediation block
covers host-key-verification failures (ssh-keyscan-based fix).

Options:
  --check-only    Report broken symlinks, uninitialized skills-vendor/
                  submodules and unregistered hooks but make no changes:
                  no submodule init, no self-sync. Exits 1 for any of the
                  three, EXCEPT a submodule .gitmodules holds with
                  'update = none' — the exit code covers everything this
                  mode reports as damage, including the wiring gap the
                  default invocation only warns about (#231). (The
                  archive-checkout path when .git is absent overrides the
                  reporting, printing its own diagnosis — it makes no
                  changes either.)
  --no-preflight  Skip the SSH pre-flight ping. Useful when the operator
                  knows the agent state and doesn't want the 3-second
                  ConnectTimeout on every invocation.
  --verbose, -v   Print resolution details even when healthy.
  --version       Print the script's diagnostic version stamp and exit.
  --help, -h      Show this help and exit.

Exit codes:
  0  All scanned symlinks resolve (or neither directory exists).
  1  One or more symlinks remain broken after self-heal attempt, or
     pre-flight SSH check failed. Under --check-only, also: an installed
     hook that .claude/settings.json does not register (#231).
  2  Invalid invocation (e.g. unknown flag).
EOF
      exit 0
      ;;
    *)
      echo "doctor.sh: unknown option: $arg" >&2
      echo "Try 'bash .skills/doctor.sh --help' for usage." >&2
      exit 2
      ;;
  esac
done

# Resolve the project root. The doctor is normally invoked from the repo
# root, but we tolerate being called from a subdirectory.
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

# Re-sync the installed copy of this script from the vendored source.
#
# The doctor is installed as a real file rather than a symlink so it stays
# reachable when the vendor submodule is uninitialized — the exact state it
# exists to repair. The cost of that choice is drift: upstream fixes reach a
# consumer only when something re-runs install-doctor.sh. The auto-refresh
# hook does that each session, but consumers that declined the hook would
# otherwise run a stale doctor indefinitely (issue #84). Since every
# reviewing-* / shipping-* preflight invokes the doctor, this is the one code
# path guaranteed to run in a hook-less consumer.
#
# The vendored copy is authoritative and CONTENT decides, not mtime. Git sets
# mtimes at checkout time, so a freshly-initialized submodule always looks
# "newer" and a deliberate submodule rollback always looks "older" — an mtime
# comparison misfires in both directions. Content equality also gives pinning
# the right semantics: a consumer pinned to an older submodule gets that
# older doctor, which is what pinning means.
#
# Wholly best-effort. A missing vendor, a missing installer, a destination
# the installer refuses to clobber, or a failed copy must all leave this
# script's exit code untouched: Phase 1 preflights invoke the doctor with
# `|| exit 1`, so a self-sync failure would otherwise block a review over
# something cosmetic.
sync_self() {
  # --check-only is contractually non-mutating — it is the mode a CI health
  # probe reaches for, and .skills/doctor.sh is a tracked file, so a write
  # here would dirty the working tree and trip a `git diff --exit-code`
  # cleanliness gate on the next submodule bump, with nothing connecting the
  # failure back to the bump.
  if [ "$CHECK_ONLY" = "1" ]; then
    return 0
  fi

  local self="$ROOT/.skills/doctor.sh"
  # Only refresh an already-installed doctor — never create one. Installation
  # is Step 2c's job (and the hook's); a preflight shouldn't materialize files
  # the operator didn't ask for. In the preflight path this always exists,
  # since that path tests `-x .skills/doctor.sh` before invoking us.
  [ -f "$self" ] || return 0

  local src installer out rc
  # First matching vendor wins, matching skills-submodule-update.sh's `break`.
  # When skills-vendor/ is absent the glob stays unexpanded and the -f test
  # rejects the literal string, so the loop falls through to `return 0`.
  for src in skills-vendor/*/skills/managing-skills/scripts/doctor.sh; do
    [ -f "$src" ] || continue
    if cmp -s "$src" "$self"; then
      return 0
    fi
    installer="$(dirname "$src")/install-doctor.sh"
    [ -f "$installer" ] || return 0

    # Capture rather than discard. A permanently-failing sync is otherwise
    # invisible: a consumer with a user-authored file at .skills/doctor.sh
    # can never receive doctor updates, and the installer's precise
    # explanation of why would go to /dev/null on every preflight forever.
    # Surfaced only under --verbose so the default path stays quiet.
    # `--quiet` means a successful run produces no output, so `out` is
    # non-empty only when something went wrong.
    rc=0
    out="$(bash "$installer" --quiet 2>&1)" || rc=$?
    if [ "$rc" -eq 0 ]; then
      echo "doctor: refreshed .skills/doctor.sh from $src — the update applies from the next run" >&2
    elif [ "$VERBOSE" = "1" ]; then
      echo "doctor: self-sync failed (rc=$rc); the installed copy is unchanged:" >&2
      printf '%s\n' "$out" >&2
    fi
    return 0
  done
  return 0
}

# Call site 1 of 2: the healthy path exits early a few lines below, which is
# the overwhelming majority of invocations. A self-sync placed after the heal
# logic would almost never run.
sync_self

# An installed hook's contract is TWO artifacts — the symlink and the
# SessionStart registration in .claude/settings.json — and only the second one
# makes it run. A repo carrying the symlink without the registration looks
# installed to anyone who lists .claude/hooks/ and runs nothing.
#
# Four of twelve audited consumers were in exactly that state for the refresh
# hook, pinned at one commit for over a week while the rest of the cohort moved
# through four skill versions (#167). Nothing detected it, because a
# half-installed hook is silent by construction: the missing half is the half
# that would have run.
#
# EVERY hook, not the one this check was written for (#224). It was hardcoded
# to skills-submodule-update.sh, so a consumer with three installed hooks and
# zero registrations had one of the three reported and the two init-socraticode
# hooks were undetectable. socraticode-health.sh is the worst one to lose that
# way — it is silent when clean by design, so "installed, unregistered, never
# runs" and "installed, registered, nothing to report" produce byte-identical
# observable behaviour. The repo that most needs the check is the one that
# cannot tell it stopped. Paired with #222, which deletes a registration
# silently, the two were a complete silent-failure loop.
#
# Reported HERE, for the same reason sync_self lives here — the doctor is the
# one code path that still runs in a repo whose hooks do not, whether through a
# reviewing-*/shipping-* preflight or a SessionStart entry of its own. Warn
# only; a wiring gap is not a dangling symlink and must not change this
# script's exit code, which Phase 1 preflights gate on with `|| exit 1`.

# The per-hook manifest a skill ships beside the script it installs: one line of
# install-hook.sh arguments, printed as the repair (#224).
#
# A hook->installer table inside this file was the smaller change and was
# rejected: it needs an edit here every time any skill adds a hook, and the
# constants would sit in a different skill from the hook they configure — the
# opposite of where #200 moved them. A manifest costs a file per hook and no
# edits ever.
#
# Found from the symlink's own target, so a manifest is located by the same
# chain the hook is: whichever vendor tree this particular link points into is
# the one whose constants apply. Prints nothing when the skill ships no
# manifest, which the caller reports differently rather than falling silent.
hook_manifest_args() {
  local hook="$1" target dir name manifest args
  target="$(readlink "$hook")"
  case "$target" in
    # A target with no directory component at all: a sibling of the link.
    */*) dir="${target%/*}" ;;
    *)   dir="." ;;
  esac
  # A relative target is relative to the LINK's directory, not to the repo root
  # this script cd'd to. `.claude/hooks/../../skills-vendor/…` then resolves for
  # the -f test below exactly as the kernel resolves the link itself.
  case "$target" in
    /*) ;;
    *) dir="${hook%/*}/$dir" ;;
  esac
  # The extension is stripped from the BASENAME. `${target%.*}` would strip
  # from the last dot anywhere in the path, so a vendor directory carrying a
  # dot in its name (skills-vendor/<owner>-<repo>) would silently produce a
  # manifest path that never matches, and the hook would quietly drop to the
  # generic repair line with nothing to say why.
  name="${target##*/}"
  manifest="$dir/${name%.*}.install"
  [ -f "$manifest" ] || return 0
  # The first line that is neither blank nor a comment, so a manifest can
  # explain itself to the next reader. `|| true` because grep exits 1 on a
  # manifest that is nothing but comments, and nothing here may be fatal.
  args="$(grep -v -e '^[[:space:]]*#' -e '^[[:space:]]*$' "$manifest" \
          | head -n 1 || true)"
  # Bounded charset, checked here rather than trusted. This string is printed
  # into a command an operator is invited to paste under pressure, and the
  # repair line is the last place anyone re-reads. Every flag and value the
  # installer accepts is already within [A-Za-z0-9._-] (it enforces that on its
  # own arguments), so a manifest outside it is malformed, not exotic.
  case "$args" in
    "" | *[!A-Za-z0-9._\ -]*) return 0 ;;
  esac
  printf '%s' "$args"
}

# Set by check_hook_registrations when any warning above printed. The default
# mode never reads it — the warning IS the whole default-mode behaviour — but
# --check-only exits 1 on it (#231): that mode is a deliberate CI probe, not a
# review preflight, so it is the one audience the detected state may gate.
REG_GAPS=0

check_hook_registrations() {
  local settings=".claude/settings.json"
  [ -d ".claude/hooks" ] || return 0
  # No jq, no warning. The doctor is advisory and runs on every session start,
  # so a wrong warning is worse than none: it would fire in every consumer
  # without jq, including correctly-installed ones, and train the reader to
  # ignore the message. install-hook.sh --check reports UNKNOWN in that case,
  # which is the right place for a demand that jq be installed.
  command -v jq >/dev/null 2>&1 || return 0

  local hook base args scope installer found
  for hook in .claude/hooks/*; do
    # A symlink, and one that resolves.
    #
    # Present-and-resolving is what distinguishes "an installer put this here
    # and it half-landed" from the two states that are not this function's
    # business: a regular file is a hook the project wrote itself (or a
    # --copy-fallback install, which install-hook.sh --check owns because
    # nothing here can see it), and a dangling symlink belongs to the scan
    # below, which names it with the repair that actually applies. Two
    # diagnoses for one file is worse than one, and nagging the group that is
    # fine trains everyone to skim past the group that is not.
    #
    # Written as a negated `if` rather than `A && B || continue`: that form is
    # not if-then-else (SC2015) and would `continue` whenever the -e test
    # failed for any reason, which is the same thing here only by accident.
    if [ ! -L "$hook" ] || [ ! -e "$hook" ]; then
      continue
    fi
    base="${hook##*/}"
    args="$(hook_manifest_args "$hook")"

    if [ -n "$args" ]; then
      # A manifest means install-hook.sh installed this, and install-hook.sh
      # writes SessionStart and nothing else — so an entry under another event
      # is not the registration this hook needs.
      scope='[.hooks.SessionStart[]?.hooks[]?.command // ""]'
    else
      # Without one, nothing declares which event the hook wants, and
      # .claude/hooks/ holds hooks for every event — this file's own header
      # describes one firing on Edit|Write|MultiEdit. Only "registered under no
      # event at all" is defensible about a hook we know nothing else about.
      scope='[.hooks[]?[]?.hooks[]?.command // ""]'
    fi
    # NOT `[ -f "$settings" ] || continue`. No settings.json at all is the
    # strongest form of unregistered, so it must fall through to the warning
    # rather than out of the loop — an early return there made the doctor
    # silent on the plainest half-install there is.
    #
    # Scoped through jq to the command strings, not a grep over the file. The
    # basename appears in settings.json for reasons that are not registrations
    # — a `permissions.allow` entry naming the hook is the common one — and a
    # whole-file grep counted those, so this warning stayed silent on exactly
    # the half-installed repos it was added for (CR finding 1).
    if [ -f "$settings" ] && jq -e --arg b "$base" "$scope
           | any(contains(\$b))" "$settings" >/dev/null 2>&1; then
      continue
    fi

    REG_GAPS=1
    echo "doctor: $hook is installed but $settings does not register it," >&2
    echo "doctor: so Claude Code never runs it and whatever it maintains is" >&2
    echo "doctor: frozen at whatever state it was in when the hook was" >&2
    echo "doctor: installed. Repair with:" >&2
    if [ -z "$args" ]; then
      # The honest degradation: the defect is still named, only the exact
      # command is not, because the skill that vendors this hook ships no
      # manifest to read it from.
      echo "doctor:   re-run the installer for the skill that vendors" >&2
      echo "doctor:   $(readlink "$hook") — it ships no ${base%.*}.install" >&2
      echo "doctor:   manifest, so the arguments cannot be named here." >&2
      continue
    fi
    # Resolved here rather than printed as a glob. `bash skills-vendor/*/…`
    # passes every extra match as an argument to the first, and install-hook.sh
    # rejects unknown arguments — so the paste-under-pressure path would fail
    # on any repo vendoring a second skills repo.
    found=0
    for installer in skills-vendor/*/skills/managing-skills/scripts/install-hook.sh; do
      [ -f "$installer" ] || continue
      echo "doctor:   bash $installer $args" >&2
      found=1
      break
    done
    [ "$found" = "1" ] || \
      echo "doctor:   bash <vendor>/skills/managing-skills/scripts/install-hook.sh $args" >&2
  done
  return 0
}

check_hook_registrations

# frontmatter_value <file> <key> — the value of the first `<key>:` line inside
# the file's YAML frontmatter, surrounding double quotes stripped; empty when
# absent. Not a YAML parser, deliberately: the doctor's dependency set is bash,
# git and (optionally) jq, and the override recipe writes flat `key: value`
# lines under `metadata:`, which a line scan reads exactly. Anything more
# exotic reads as absent — and an absent field is REPORTED as un-assessable
# below, never mis-assessed. Matching is prefix-exact on the key, so
# `overrides:` cannot match `override-reason:`.
frontmatter_value() {
  local file="$1" key="$2" val
  val="$(awk -v k="$key" '
    NR == 1 { if ($0 !~ /^---[[:space:]]*$/) exit; next }
    /^---[[:space:]]*$/ { exit }
    {
      line = $0
      sub(/\r$/, "", line)
      sub(/^[[:space:]]+/, "", line)
      if (index(line, k ":") == 1) {
        line = substr(line, length(k) + 2)
        sub(/^[[:space:]]+/, "", line)
        print line
        exit
      }
    }
  ' "$file")"
  val="${val%\"}"
  val="${val#\"}"
  printf '%s' "$val"
}

# The two voices of the drift check (#238), shared so every case below agrees
# about the diagnosis. Drift names both sides and the re-sync DIRECTION —
# reapplying upstream changes onto the old fork instead of local deltas onto
# the new upstream text is the easy inversion, and it quietly discards every
# release between the two. Un-assessable is its own warning rather than a
# silent skip: an override nothing can compare is the same failure as not
# detecting drift at all.
report_override_drift() {
  local dir="$1" target="$2" have="$3" now="$4"
  echo "doctor: $dir overrides $target and has fallen behind: last synced" >&2
  echo "doctor: at $have, vendor now at $now. Re-sync by reapplying the" >&2
  echo "doctor: local deltas onto the newer upstream text — never upstream" >&2
  echo "doctor: changes onto the old fork — then bump the override's" >&2
  echo "doctor: version:/synced-from: to what was just synced. Advisory:" >&2
  echo "doctor: nothing is auto-merged." >&2
}

report_override_unassessed() {
  local dir="$1" target="$2" why="$3"
  echo "doctor: $dir overrides $target but its drift cannot be assessed:" >&2
  echo "doctor: $why. An override nothing can compare is the same failure" >&2
  echo "doctor: as not detecting drift at all (#238)." >&2
}

# #238 — a local override is the one file the drift mitigations cannot reach.
# The auto-refresh hook moves the submodule pointer, which never touches a
# forked file; per-script symlinks track upstream for free; and the symlink
# scans skip regular directories by construction. So an override fell further
# behind on every release and the only detector was a consumer hitting a
# broken instruction: one sat at v1.2 against vendor's v1.4 and reintroduced
# #63's exact failure sixteen months after it was closed, because the file
# carrying the fix was the fork.
#
# The frontmatter already carries the machine-readable link, from the override
# recipe: `overrides:` names the vendor path and `version:` sits beside it.
# `version:` in an override records THE VENDOR VERSION LAST SYNCED FROM — not
# a version of the local file — bumped on every re-sync even when the local
# deltas are unchanged; the two readings diverge as soon as someone edits an
# override after syncing, which is an override's whole job. For a vendor that
# ships no version: at all, the `synced-from:` sibling key pins the vendor
# commit last synced from ("<repo> <tag> (<commit>)"), and the comparison is a
# diff between that commit and HEAD scoped to the skill's path — so a
# submodule bump that touches OTHER skills stays silent rather than training
# the reader to skim.
#
# Warn only, in every mode. Never an exit code — not even under --check-only,
# whose gate covers damage and wiring gaps (#231); drift is doc-sync debt the
# operator pays down on their schedule, and a probe that failed on it would
# push consumers toward deleting overrides rather than re-syncing them. Never
# an auto-merge — upstream text cannot be applied to a fork blindly.
check_override_drift() {
  [ -d skills ] || return 0
  local dir md target repo_dir skill_rel vendor_md o_ver v_ver synced rec rc
  for dir in skills/*; do
    # A regular directory carrying a SKILL.md whose frontmatter names an
    # overrides: target. Symlinked skills track upstream by construction, and
    # a local directory without the key is project-authored — a fork of
    # nothing, with nothing to fall behind.
    if [ -L "$dir" ] || [ ! -d "$dir" ] || [ ! -f "$dir/SKILL.md" ]; then
      continue
    fi
    md="$dir/SKILL.md"
    target="$(frontmatter_value "$md" overrides)"
    [ -n "$target" ] || continue

    repo_dir="skills-vendor/${target%%/*}"
    skill_rel="skills/${target#*/}"
    vendor_md="$repo_dir/$skill_rel/SKILL.md"
    if [ ! -f "$vendor_md" ]; then
      report_override_unassessed "$dir" "$target" \
        "no vendor copy at $vendor_md (uninitialized submodule, or the skill moved upstream)"
      continue
    fi

    v_ver="$(frontmatter_value "$vendor_md" version)"
    o_ver="$(frontmatter_value "$md" version)"
    if [ -n "$v_ver" ]; then
      if [ -z "$o_ver" ]; then
        report_override_unassessed "$dir" "$target" \
          "the vendor is at version $v_ver and the override records no version: (the vendor version last synced from)"
      elif [ "$o_ver" != "$v_ver" ]; then
        report_override_drift "$dir" "$target" "version $o_ver" "version $v_ver"
      fi
      continue
    fi

    # Unversioned upstream: the synced-from fallback.
    synced="$(frontmatter_value "$md" synced-from)"
    if [ -z "$synced" ]; then
      report_override_unassessed "$dir" "$target" \
        "the vendor ships no version: and the override records no synced-from: (\"<repo> <tag> (<commit>)\")"
      continue
    fi
    rec="${synced##*(}"
    rec="${rec%%)*}"
    if [ "$rec" = "$synced" ] || [ -z "$rec" ]; then
      report_override_unassessed "$dir" "$target" \
        "synced-from: \"$synced\" carries no (commit) to compare against"
      continue
    fi
    if ! git -C "$repo_dir" rev-parse --verify --quiet "$rec^{commit}" >/dev/null 2>&1; then
      report_override_unassessed "$dir" "$target" \
        "the recorded commit $rec is not in the vendor's history (shallow clone?)"
      continue
    fi
    # diff --quiet: 0 unchanged, 1 changed, anything else is an error — which
    # must land in "cannot be assessed" rather than in either verdict.
    rc=0
    git -C "$repo_dir" diff --quiet "$rec" HEAD -- "$skill_rel" 2>/dev/null || rc=$?
    if [ "$rc" -eq 1 ]; then
      report_override_drift "$dir" "$target" "commit $rec" "a vendor tree that has since changed $skill_rel"
    elif [ "$rc" -ne 0 ]; then
      report_override_unassessed "$dir" "$target" \
        "'git diff $rec HEAD -- $skill_rel' failed in $repo_dir"
    fi
  done
  return 0
}

check_override_drift

# #256 — a vendored file committed as a REGULAR FILE where a symlink was
# expected stops tracking upstream forever, and nothing detected it: not this
# doctor, not managing-skills, not the consumer's own tooling.
#
# Two vendoring shapes are in use across the cohort and they look identical
# from a shell, which is why this survived several sweeps:
#
#   - a whole-directory symlink at skills/<name> — everything beneath it is
#     upstream by construction, and nothing can drift;
#   - a real directory of PER-FILE symlinks — a change reaches it file by
#     file, so any one file committed as 100644/100755 silently opts out.
#
# The cost, measured: cannabis.observer-wordpress carried
# skills/shipping-work-php/scripts/doc-check.sh as a regular file among five
# symlinked siblings, still running the pre-#252 matcher, with its own tailored
# path list matching nothing — the exact bug #252 fixed, sitting undetected in
# the repo that had tailored the list most carefully. Three others carried a
# forked SKILL.md, so their scripts updated and the instructions describing
# them did not.
#
# Reported, never healed. A project-local divergence is sometimes deliberate,
# so the remedy is named and the operator decides. Two ways to say "deliberate":
# pre-ship.sh by name (upstream ships a stub for the bare variant, and
# docs/STYLE.md blesses a project-supplied wrapper), and a .skills/forked-ok
# list for anything else.
#
# Warn-only in every mode, including --check-only — same call as the override
# drift above it (#238). A fork is sync debt an operator pays down on their
# schedule, and a probe that failed on it would push consumers toward deleting
# the local file rather than declaring it.

# Basenames a consumer is expected to supply itself. Space-delimited, matched
# whole.
FORK_EXEMPT_NAMES=" pre-ship.sh "

# A declared-deliberate fork, one repo-relative path per line, `#` comments and
# blank lines ignored. Same shape and the same no-trailing-newline guard as
# doc-check.sh's .skills/doc-sensitive-paths.
fork_declared() {
  local want="$1" line
  [ -f .skills/forked-ok ] || return 1
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|\#*) continue ;; esac
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [ "$line" = "$want" ] && return 0
  done < .skills/forked-ok
  return 1
}

# FORKED is check_silent_forks' output channel, declared at top scope for the
# same reason BROKEN and UNINIT are. One remedy block for the whole list, not
# one per file: the three repos that forked a SKILL.md would otherwise print
# the same six lines for every file they link.
#
# Two parallel arrays rather than one "path (vendor: path)" string, because the
# consumer path is the exact text .skills/forked-ok has to hold. The combined
# form printed a line that could not be pasted into the ack file — the obvious
# and only implied workflow — and the paste then declared nothing, silently,
# with the doctor reporting the same fork again next run (#256 CR round 1). An
# escape hatch that quietly does not apply is the defect it exists to declare.
declare -a FORKED=()
declare -a FORKED_VENDOR=()

report_forked() {
  local i
  echo "doctor: silently forked — regular files where the vendor ships one:" >&2
  for i in "${!FORKED[@]}"; do
    echo "  ${FORKED[$i]}" >&2
    echo "      (upstream: ${FORKED_VENDOR[$i]})" >&2
  done
  echo "doctor: they will never receive upstream fixes, and nothing else" >&2
  echo "doctor: detects that. Replace each with a relative symlink into" >&2
  echo "doctor: skills-vendor/ (wrap, don't fork — docs/STYLE.md), or declare" >&2
  echo "doctor: it deliberate in .skills/forked-ok — one path per line, copied" >&2
  echo "doctor: exactly as listed above and nothing else on the line; the" >&2
  echo "doctor: (upstream: …) line is context, not part of the path (#256)." >&2
  echo "doctor: Advisory: nothing is changed for you." >&2
}

check_silent_forks() {
  FORKED=()
  FORKED_VENDOR=()
  [ -d skills ] || return 0
  local dir name vendor_dir vendor_file rel local_file
  for dir in skills/*; do
    # A whole-directory symlink cannot fork. A declared override is local by
    # definition — its drift is check_override_drift's business, not this
    # one's, and reporting every file in it would bury the real finding.
    [ -L "$dir" ] && continue
    [ -d "$dir" ] || continue
    if [ -f "$dir/SKILL.md" ] \
      && [ -n "$(frontmatter_value "$dir/SKILL.md" overrides)" ]; then
      continue
    fi
    name="${dir#skills/}"
    for vendor_dir in skills-vendor/*/skills/"$name"; do
      [ -d "$vendor_dir" ] || continue
      # The three directories the spec defines, plus the SKILL.md itself.
      # Enumerated by glob rather than by `find` so this needs no subprocess
      # and no exit code to check: an unmatched glob stays literal and the
      # -f test rejects it.
      for vendor_file in "$vendor_dir"/*.md "$vendor_dir"/scripts/* \
        "$vendor_dir"/references/* "$vendor_dir"/assets/*; do
        [ -f "$vendor_file" ] || continue
        rel="${vendor_file#"$vendor_dir"/}"
        case "$FORK_EXEMPT_NAMES" in *" ${rel##*/} "*) continue ;; esac
        local_file="$dir/$rel"
        # Absent is not forked: a consumer that links only some of a skill's
        # files is using less of it, not diverging from it.
        [ -e "$local_file" ] || continue
        [ -L "$local_file" ] && continue
        [ -f "$local_file" ] || continue
        fork_declared "$local_file" && continue
        FORKED+=("$local_file")
        FORKED_VENDOR+=("$vendor_file")
      done
      # First matching vendor wins, as everywhere else in this script.
      break
    done
  done
  if [ "${#FORKED[@]}" -gt 0 ]; then
    report_forked
  fi
  return 0
}

# Call site 1 of 2. A fresh clone has no vendor tree to compare against, so
# this pass is silent there and the one below — after the heal, where the tree
# has only just become readable — is the one that reports. Same two-call shape
# sync_self uses, for the same reason: the check that needs the vendor content
# cannot run before the content exists, and a fresh clone or a new worktree is
# exactly where a consumer runs the doctor first (#256 CR round 1).
check_silent_forks

# #231 — the audience split, enforced. The unregistered-hook state was detected
# (#224) and still ungated: every review preflight runs the default invocation,
# whose exit code cannot change without hard-blocking nine skills' reviews in
# every affected consumer over a defect that is not in the diff under review —
# the same absent-vs-unusable failure #140 removed from the shellcheck gate. So
# the severity stays and the audience splits: --check-only is a deliberate
# probe, and a probe that reports damage and then signals "fine" to the CI job
# branching on it is the #185 shape all over again. Called at every point
# --check-only would otherwise exit 0; the default mode always falls through.
# Finer-grained alternative for consumers gating per hook:
# `install-hook.sh --check` exits 3 for exactly one hook's wiring.
check_only_registration_gate() {
  if [ "$CHECK_ONLY" = "1" ] && [ "$REG_GAPS" = "1" ]; then
    echo "doctor: --check-only: the unregistered hook(s) above fail this" >&2
    echo "doctor: probe (#231). The default invocation still warns and" >&2
    echo "doctor: exits 0, so review preflights are unaffected." >&2
    exit 1
  fi
  return 0
}

# Directories whose direct children are scanned for dangling symlinks.
# skills/ is the vendored-skill chain; .claude/hooks/ holds the hook symlinks
# skill installers write into a consumer (issue #99). .claude/skills/ needs no
# entry — it links through skills/<name>, so a break there is already reported
# at its source rather than twice.
SCAN_DIRS=(skills .claude/hooks)

# An `if`, not `[ -d "$d" ] && present=1` — under `set -e` the && form makes the
# loop's exit status that of the last test, so a run whose final scan dir is
# absent would abort the script instead of reporting.
present=0
for d in "${SCAN_DIRS[@]}"; do
  if [ -d "$d" ]; then
    present=1
  fi
done

# Nothing to check if the consumer uses none of those patterns.
if [ "$present" -eq 0 ]; then
  [ "$VERBOSE" = "1" ] && echo "doctor: no skills/ or .claude/hooks/ directory — nothing to check" >&2
  exit 0
fi

# BROKEN is the output channel of scan_broken — declared at top scope so the
# function's communication pattern is visible without reading every call
# site. Using an array (rather than a single string) preserves
# paths-with-spaces correctly when later expanded as "${BROKEN[@]}".
declare -a BROKEN=()

# Walks each SCAN_DIRS entry and populates BROKEN with any dangling symlinks.
# A symlink is "broken" when it exists but its target does not resolve. Local
# overrides (regular directories) and project-authored hook scripts (regular
# files) are skipped — they're not symlinks.
#
# One nested level too: <dir>/*/scripts/* (#238). An override that follows the
# per-script pattern is a regular directory whose scripts/ symlinks into the
# submodule everything it does not change, and those links sit one level below
# the walk this function used to do. The blind spot is narrow but real: with an
# INITIALIZED submodule whose upstream renamed or deleted a script, every
# top-level symlink resolves and scan_uninit reports nothing, so the dangling
# nested link surfaced as `No such file or directory` mid-run — the failure
# mode this script exists to turn into an actionable message. That is the new
# risk the per-script pattern introduces, which is why this scan is a
# prerequisite of the skill recommending it. The nested pass skips top-level
# symlinks: a symlinked skill IS the healthy vendor chain, and its target's
# internals belong to the vendor tree, not to this consumer's damage report.
scan_broken() {
  BROKEN=()
  local dir sub entry
  for dir in "${SCAN_DIRS[@]}"; do
    [ -d "$dir" ] || continue
    for entry in "$dir"/*; do
      [ -L "$entry" ] || continue
      if [ ! -e "$entry" ]; then
        BROKEN+=("$entry")
      fi
    done
    for sub in "$dir"/*; do
      if [ -L "$sub" ] || [ ! -d "$sub" ]; then
        continue
      fi
      for entry in "$sub"/scripts/*; do
        [ -L "$entry" ] || continue
        if [ ! -e "$entry" ]; then
          BROKEN+=("$entry")
        fi
      done
    done
  done
}

# UNINIT is scan_uninit's output channel, declared at top scope for the same
# reason BROKEN is. UNHELD is the subset .gitmodules does NOT deliberately hold
# uninitialized, and it is the one that decides an exit code — see uninit_held.
declare -a UNINIT=()
declare -a UNHELD=()
# HELD is UNINIT minus UNHELD, built where the classification already happens
# rather than recovered later by matching one against the other. The obvious
# `case " ${UNHELD[*]} " in *" $p "*` membership test is space-DELIMITED, which
# silently defeats the space-PRESERVING prefix strip below: with UNHELD=("a b"),
# the unrelated path "a" matches (#199 CR round 2, finding 10).
declare -a HELD=()

# uninit_held <path> -> 0 when .gitmodules pins this path with `update = none`.
#
# The reason the two arrays exist. An uninitialized submodule is either a
# deliberate hold or a broken checkout, and `git submodule status` prefixes
# both with '-'. Only .gitmodules can tell them apart, and only before an init
# is attempted — which is exactly the position --check-only is in, since it
# makes no changes and therefore never gets to observe what survives a repair.
#
# Verified rather than assumed: with `update = none`, `git submodule update
# --init --recursive` registers the path, prints "Skipping submodule", exits 0,
# and leaves the '-' in place permanently. The heal here carries no `--merge`,
# so the hold is honoured — note that the sibling refresh hook reaches for
# .skills/skills-pin instead precisely because `--merge` overrides this (#100).
#
# A pin is NOT a hold: a pinned submodule is initialized and sits at a recorded
# commit, so it never appears here in the first place.
uninit_held() {
  local path="$1" name key value
  [ -f .gitmodules ] || return 1
  name=""
  while read -r key value; do
    [ "$value" = "$path" ] || continue
    key="${key#submodule.}"
    name="${key%.path}"
    break
  done < <(git config -f .gitmodules --get-regexp '^submodule\..*\.path$' 2>/dev/null || true)
  [ -n "$name" ] || return 1
  [ "$(git config -f .gitmodules --get "submodule.$name.update" 2>/dev/null || true)" = "none" ]
}

# Populates UNINIT with skills-vendor/ submodules recorded in the index that
# git has not initialized. `git submodule status` prefixes exactly those with
# '-', and it says so whether or not their content happens to be on disk —
# which is what makes it the right probe for the half-healed checkout that the
# symlink scan above cannot see (#185, header note).
#
# Scoped to skills-vendor/, unlike the repo-wide heal it triggers. The doctor
# exists for the vendored-skill chain; a project may carry an unrelated
# submodule it deliberately leaves uninitialized, and a Phase 1 preflight that
# cloned it before every review would be a worse defect than the one this
# closes. When a skills-vendor/ submodule *does* need repair, the existing
# repo-wide command runs — the same blast radius the dangling-symlink path has
# always had.
#
# #100's pin filter needs nothing here. The heal is `--init --recursive` with
# no `--remote`, so it can only check out the gitlink the superproject already
# records — which is precisely what a pin holds. Initializing a pinned
# submodule is fine; refreshing it is not, and this cannot refresh.
scan_uninit() {
  UNINIT=()
  UNHELD=()
  HELD=()
  # Absent skills-vendor/ means this consumer doesn't use the pattern; skip
  # the git call entirely rather than reason about its output.
  [ -d skills-vendor ] || return 0

  # stdout only, and never fatal. Outside a git checkout, or with a pathspec
  # matching no submodule, git complains on stderr — neither is a doctor
  # failure, and this probe must not perturb an exit code the Phase 1
  # preflights gate on.
  local status_out line
  status_out="$(git submodule status -- skills-vendor/ 2>/dev/null || true)"
  while IFS= read -r line; do
    case "$line" in
      -*) ;;
      *) continue ;;
    esac
    # Shortest-match strip of the '-<sha> ' prefix, so a submodule path
    # containing a space survives intact — `awk '{print $2}'` truncates it.
    local path="${line#-* }"
    UNINIT+=("$path")
    if uninit_held "$path"; then HELD+=("$path"); else UNHELD+=("$path"); fi
  done <<UNINIT_EOF
$status_out
UNINIT_EOF
}

# Reports UNINIT and what it means. Shared by --check-only and the post-heal
# re-check, which must not disagree about the diagnosis.
#
# Held paths are named separately, and named as expected rather than as
# damage. Reporting a deliberate `update = none` in the same breath as a broken
# checkout is what would train a reader to skim past both.
report_uninit() {
  echo "doctor: skills-vendor/ submodules recorded but not initialized:" >&2
  printf '  %s\n' "${UNINIT[@]}" >&2
  if [ "${#HELD[@]}" -gt 0 ]; then
    echo "doctor: of those, .gitmodules holds these with 'update = none', which" >&2
    echo "doctor: is deliberate and stays uninitialized through any repair:" >&2
    printf '  %s (held)\n' "${HELD[@]}" >&2
  fi
  echo "doctor: their vendored skills are unreachable, and a bare" >&2
  echo "doctor: 'git submodule update --remote --merge' will skip them and still" >&2
  echo "doctor: exit 0 — pass --init (#176)." >&2
}

scan_broken
scan_uninit

if [ "${#BROKEN[@]}" -eq 0 ] && [ "${#UNINIT[@]}" -eq 0 ]; then
  check_only_registration_gate
  # Names both checks, because the fast path now clears both. The old wording
  # claimed only the symlink half, which is the substitution this fix removed.
  [ "$VERBOSE" = "1" ] && \
    echo "doctor: all scanned symlinks resolve; skills-vendor/ submodules initialized" >&2
  exit 0
fi

# At least one dangling symlink. Distinguish archive-checkout (no .git) from
# the normal git-submodule case before either self-healing or reporting,
# so --check-only never suggests a `git submodule` command in a checkout
# that doesn't have a .git dir.
#
# Guarded on BROKEN because this branch prints BROKEN and nothing else. It is
# already unreachable with an empty one — UNINIT can only be populated where
# `git submodule status` works, which requires the .git this branch tests for
# — but the invariant is cheaper to state than to re-derive.
if [ "${#BROKEN[@]}" -gt 0 ] && [ ! -d .git ] && [ ! -f .git ]; then
  echo "doctor: dangling symlinks detected and no .git directory present:" >&2
  printf '  %s\n' "${BROKEN[@]}" >&2
  echo "" >&2
  echo "This checkout was likely created from a source archive (zip/tarball)" >&2
  echo "rather than 'git clone'. The submodule pattern this repo uses is not" >&2
  echo "compatible with archive downloads. Clone with --recurse-submodules" >&2
  echo "instead, or vendor the skill scripts manually." >&2
  exit 1
fi

if [ "$CHECK_ONLY" = "1" ]; then
  if [ "${#UNINIT[@]}" -gt 0 ]; then
    report_uninit
  fi
  if [ "${#BROKEN[@]}" -gt 0 ]; then
    echo "doctor: dangling symlinks detected:" >&2
    printf '  %s\n' "${BROKEN[@]}" >&2
    echo "Run 'git submodule update --init --recursive' to repair." >&2
    exit 1
  fi
  # An UNHELD path fails this mode, and it is the whole reason uninit_held
  # exists. --check-only changes nothing, so it never gets to see what survives
  # a repair — which means the exemption has to be read out of .gitmodules
  # here, or this mode reports the half-healed checkout and then signals "fine"
  # to the CI job branching on its exit code. That silence is the shape of the
  # defect #185 was filed for: something exited 0 and a consumer sat broken.
  if [ "${#UNHELD[@]}" -gt 0 ]; then
    echo "doctor: the paths above are not held by 'update = none' — this" >&2
    echo "doctor: checkout is half-healed. Run 'git submodule update --init" >&2
    echo "doctor: --recursive', or drop --check-only to self-heal." >&2
    exit 1
  fi
  check_only_registration_gate
  exit 0
fi

# Targeted remediation printed when an auth failure is detected — either by
# the pre-flight ping below or by classifying submodule init's stderr.
# Kept generic across hosts (github.com is the dominant case but we don't
# hard-code it in the message) and explicit about each layer the user
# might need to fix: agent reachable, key loaded, agent visible to
# subprocesses, fallback to HTTPS.
print_ssh_remediation() {
  cat >&2 <<'EOF'

doctor: the auth check above failed — auth to one of the submodule
remotes was refused. Common causes (and fixes):

  1. SSH agent not reachable from this shell.
       ssh-add -l
     "Error connecting to authentication agent" → start the agent and
     re-add keys.

  2. Agent has no identities loaded.
     Add the default key (macOS keychain integration):
       ssh-add --apple-use-keychain ~/.ssh/id_ed25519
     (adjust the key path if yours isn't id_ed25519 — `ls ~/.ssh/id_*`
     shows what's available.)
     And persist via ~/.ssh/config so the key auto-loads:
       Host github.com
         AddKeysToAgent yes
         UseKeychain yes
         IdentityFile ~/.ssh/id_ed25519

  3. Auth works in your terminal but not from a wrapper script.
       ssh -T git@github.com
     If that succeeds interactively but fails here, a wrapper in the
     chain (e.g. dev.sh) is scrubbing SSH_AUTH_SOCK from the env.

  4. Public submodule, no SSH credentials available.
     Force HTTPS for github.com globally (affects ALL repos on this
     machine — use only if you understand the scope):
       git config --global url."https://github.com/".insteadOf "git@github.com:"

EOF
}

# Companion remediation for the host-key-verification failure path. Surfaced
# when the pre-flight ping under StrictHostKeyChecking=yes is rejected
# because the remote's host key isn't in ~/.ssh/known_hosts. The agent
# remediation above doesn't apply here — the operator just needs to trust
# the host key.
print_host_key_remediation() {
  cat >&2 <<'EOF'

doctor: SSH refused to talk to one of the submodule remotes because its
host key isn't in ~/.ssh/known_hosts. Trust the host key (verify the
fingerprint against the forge's published list first):

  ssh-keyscan github.com >> ~/.ssh/known_hosts

GitHub publishes its current host-key fingerprints at
https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints —
compare the ssh-keyscan output before appending. For other forges, check
their docs for the equivalent.

EOF
}

# Pre-flight: when .gitmodules references SSH remotes, verify SSH auth to
# each referenced host before attempting submodule init. Catches the
# common "agent not reachable / key not loaded" case with a clean
# message instead of letting submodule init produce a wall of clone
# errors. Skipped when .gitmodules is HTTPS-only or absent, and skippable
# entirely with --no-preflight.
preflight_ssh_check() {
  [ "$NO_PREFLIGHT" = "1" ] && return 0
  [ -f .gitmodules ] || return 0

  # Extract unique host names from SSH-style submodule URLs:
  #   url = git@<host>:<path>
  #   url = ssh://git@<host>[:<port>]/<path>
  local hosts
  hosts="$(awk '
    /^[[:space:]]*url[[:space:]]*=[[:space:]]*git@/ {
      # url = git@host:path → split on @ and :
      sub(/^[^@]*@/, "", $0); sub(/:.*$/, "", $0); print
    }
    /^[[:space:]]*url[[:space:]]*=[[:space:]]*ssh:\/\/git@/ {
      # url = ssh://git@host[:port]/path → strip scheme+user, then host
      sub(/^.*@/, "", $0); sub(/[:\/].*$/, "", $0); print
    }
  ' .gitmodules | sort -u)"
  [ -n "$hosts" ] || return 0

  local host out failed=0
  local -a auth_failed=()
  local -a hostkey_failed=()
  for host in $hosts; do
    # GitHub (and most forges) return exit 1 even on successful auth
    # because there's no shell to allocate ("PTY allocation request
    # failed" / "successfully authenticated"). The reliable signal is in
    # the output, not the exit code. Two failure modes worth
    # distinguishing:
    #
    #   - "Permission denied (...)" — auth refused. The parenthesized
    #     methods vary by server config (publickey / password /
    #     publickey,password,keyboard-interactive / etc.), so we match
    #     on the open-paren form to catch all variants without false-
    #     positiving on banners that happen to contain the literal
    #     string "Permission denied".
    #   - "Host key verification failed" — server's host key isn't in
    #     known_hosts. We run with StrictHostKeyChecking=yes
    #     deliberately: the alternative (accept-new) silently expands
    #     the operator's known_hosts on first contact, which is a
    #     security choice this script shouldn't make on the operator's
    #     behalf. BatchMode=yes prevents the script from hanging on the
    #     interactive trust prompt.
    #
    # Other failures (DNS, timeout, network) are not classified — we
    # don't want false positives that block valid retries on flaky
    # network conditions.
    out="$(ssh -T -o BatchMode=yes -o ConnectTimeout=3 \
              -o StrictHostKeyChecking=yes "git@${host}" 2>&1 || true)"
    if printf '%s\n' "$out" | grep -qE "Permission denied \("; then
      auth_failed+=("$host")
      failed=1
    elif printf '%s\n' "$out" | grep -qi "Host key verification failed"; then
      hostkey_failed+=("$host")
      failed=1
    fi
  done

  if [ "$failed" -eq 1 ]; then
    if [ "${#auth_failed[@]}" -gt 0 ]; then
      echo "doctor: SSH pre-flight failed — agent cannot authenticate to: ${auth_failed[*]}" >&2
      print_ssh_remediation
    fi
    if [ "${#hostkey_failed[@]}" -gt 0 ]; then
      echo "doctor: SSH pre-flight failed — host key not trusted for: ${hostkey_failed[*]}" >&2
      print_host_key_remediation
    fi
    return 1
  fi
  return 0
}

if ! preflight_ssh_check; then
  exit 1
fi

# Both reasons are printed, not whichever one was checked first. An operator
# watching a slow clone should see everything that provoked it — reporting one
# of two causes is how the second one gets rediscovered later as a surprise.
if [ "${#UNINIT[@]}" -gt 0 ]; then
  report_uninit
fi
if [ "${#BROKEN[@]}" -gt 0 ]; then
  echo "doctor: dangling symlinks detected — initializing submodules..." >&2
else
  echo "doctor: initializing submodules..." >&2
fi

# Capture stderr for post-hoc classification while also streaming it live
# so the user sees git's output during slow clones. We use a named pipe
# (fifo) + explicit-backgrounded tee with a known PID instead of the
# more compact `2> >(tee … >&2)` process-substitution form: bash 3.2
# (the default on macOS without Homebrew) does not track process
# substitutions in the jobs table, so `wait` can't reliably reap the
# tee, leaving a microsecond-wide race between tee's final flush and
# our grep. A real backgrounded job with `wait "$TEE_PID"` works on
# every bash from 3.2 onward. Both temp paths live inside a single
# mktemp -d directory so the trap can `rm -rf` once.
SUBMODULE_TMP="$(mktemp -d "${TMPDIR:-/tmp}/doctor-submodule.XXXXXX")"
# Install the cleanup trap before any further setup that could fail (mkfifo,
# tee &) so a partial-init failure still removes the tempdir.
trap 'rm -rf "$SUBMODULE_TMP"' EXIT
SUBMODULE_FIFO="$SUBMODULE_TMP/stderr"
SUBMODULE_ERR="$SUBMODULE_TMP/captured"
mkfifo "$SUBMODULE_FIFO"

tee "$SUBMODULE_ERR" < "$SUBMODULE_FIFO" >&2 &
TEE_PID=$!

RC=0
# When git exits, the fifo write side closes; tee reads EOF and exits.
git submodule update --init --recursive 2>"$SUBMODULE_FIFO" || RC=$?
# `|| true` so a non-zero tee exit (e.g., write error) doesn't trip set -e.
wait "$TEE_PID" || true

if [ "$RC" -ne 0 ]; then
  # Match the open-paren form of SSH auth refusal so we catch every
  # variant (publickey / password / publickey,password,keyboard-interactive
  # / ...). HTTPS auth has its own signature unrelated to SSH; "Could not
  # read from remote repository" appears for both SSH and HTTPS clone
  # failures rooted in auth.
  if grep -qE "Permission denied \(|Could not read from remote repository|Authentication failed for 'https://" "$SUBMODULE_ERR"; then
    print_ssh_remediation
  else
    echo "doctor: 'git submodule update --init --recursive' failed" >&2
  fi
  exit 1
fi

# Call site 2 of 2: the vendor tree may have only just become readable — call
# site 1 ran before the init and would have found nothing to sync against.
sync_self


# Re-check after self-heal.
scan_broken
if [ "${#BROKEN[@]}" -gt 0 ]; then
  echo "doctor: symlinks still dangling after submodule init:" >&2
  printf '  %s\n' "${BROKEN[@]}" >&2
  echo "" >&2
  echo "The .gitmodules entry for the vendor repo may be missing, or the" >&2
  echo "symlink target points to a path that does not exist upstream." >&2
  exit 1
fi

# A submodule that survives its own successful init is HELD or it is broken,
# and uninit_held is what tells them apart. Verified: with
# `submodule.<name>.update = none` in .gitmodules, `git submodule update --init
# --recursive` registers the path, prints "Skipping submodule", exits 0, and
# leaves `git submodule status` still showing '-'. That is a legitimate
# configuration, so failing on it would block every Phase 1 preflight in such a
# consumer forever, over a state where all the symlinks resolve and every skill
# is reachable.
#
# The exemption is for the held paths, not for the condition. A path that is
# NOT held and still uninitialized after a repair that was supposed to
# initialize it is an unrepaired failure, and this is the one place in the
# script that can say so with evidence — the init ran and the residue survived
# it. Blanket non-fatality here would have made the heal path quieter than
# --check-only, which is backwards: the mode that attempted a fix knows more.
scan_uninit
if [ "${#UNINIT[@]}" -gt 0 ]; then
  echo "doctor: still uninitialized after 'git submodule update --init --recursive':" >&2
  printf '  %s\n' "${UNINIT[@]}" >&2
fi
if [ "${#UNHELD[@]}" -gt 0 ]; then
  echo "doctor: and these are not held by 'update = none' in .gitmodules, so" >&2
  echo "doctor: the repair did not take:" >&2
  printf '  %s\n' "${UNHELD[@]}" >&2
  exit 1
fi

# Call site 2 of 2, for the same reason (#256): before the init there was no
# vendored skill to compare a consumer's regular file against.
#
# BELOW the post-heal gates, not above them. This report is advisory and the
# two checks above exit 1; printing a fork list ahead of a hard failure buries
# the failure under output the operator does not have to act on today.
check_silent_forks

[ "$VERBOSE" = "1" ] && echo "doctor: self-healed; all scanned symlinks resolve" >&2
exit 0
