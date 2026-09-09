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
VERSION="2026-09-04-3"

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

That version comparison checks the STAMP. It cannot see divergence at
the SAME version — an override synced honestly from v1.1 whose text
dropped a fix v1.1 contains — and #63 was reintroduced a second time
through that opening (#260). So each override's SKILL.md is also read,
whatever its stamp says, for two things:

  - fragments the vendor FENCES as required. Upstream marks a block
    <!-- skill:required --> when dropping it reintroduces a fixed
    failure; an override must carry each one, compared insensitive to
    whitespace. Only fenced code blocks, and only what upstream marks —
    an override is supposed to differ, so this is not a diff.
  - a bare "bash scripts/X.sh" invocation INSIDE A FENCED CODE BLOCK,
    where it is an instruction to execute. The agent's cwd is the
    project root and a skill's scripts/ ships inside the skill, so that
    path opens nothing (#63). A script that cd's to the toplevel does
    not fix it: that resolves the root it operates on, not the path
    bash uses to open the file. Prose is not scanned: an override
    carrying upstream's own warning against the pattern was reported
    as committing it.

Both are advisory in every mode, like the drift check they sit beside.

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
# about the diagnosis. Drift names both sides, the re-sync DIRECTION and the
# CHECK. Reapplying upstream changes onto the old fork instead of local deltas
# onto the new upstream text is the easy inversion, and it quietly discards
# every release between the two. The check is named here because this message
# is where an operator meets the re-sync at all, and the obvious verification
# — grep the merged file for what you expected — cannot see a local delta the
# merge dropped (#267); only a diff of the pre-merge override can, which is
# why the copy has to be taken before the merge overwrites it. Un-assessable
# is its own warning rather than a silent skip: an override nothing can
# compare is the same failure as not detecting drift at all.
#
# Both voices collect and report after the loop, for the reason FORKED and the
# content checks do: one remedy for the whole list, not one per finding. The
# remedy is the long half of each message and identical every time, so per-file
# printing buries the only lines that differ: three drifted overrides printed
# 27 lines of which 24 were the same paragraph. Un-assessable is the worse of
# the two to repeat — its likeliest cause is an uninitialized submodule, which
# makes EVERY override unassessable at once — and the per-case "why" carries
# the interpolated path, so it belongs on its own entry line rather than
# spliced into a hand-wrapped sentence.
declare -a DRIFTED=()
declare -a UNASSESSED=()

# One formatter per class, so the call sites cannot word the same fact
# differently: drift is recorded from the version comparison and from the
# synced-from commit comparison, un-assessable from six distinct causes.
record_override_drift() {
  local dir="$1" target="$2" have="$3" now="$4"
  DRIFTED+=("$dir overrides $target: last synced at $have, vendor now at $now")
}

record_override_unassessed() {
  local dir="$1" target="$2" why="$3"
  UNASSESSED+=("$dir overrides $target: $why")
}

report_drifted_overrides() {
  local i
  echo "doctor: an override has fallen behind its vendor:" >&2
  for i in "${!DRIFTED[@]}"; do
    echo "  ${DRIFTED[$i]}" >&2
  done
  echo "doctor: copy each override aside BEFORE merging, then re-sync by" >&2
  echo "doctor: reapplying the local deltas onto the newer upstream text —" >&2
  echo "doctor: never upstream changes onto the old fork — and bump the" >&2
  echo "doctor: override's version:/synced-from: to what was just synced." >&2
  echo "doctor: Last, diff that copy against the merged file and account for" >&2
  echo "doctor: every removed line: a presence-only check cannot see a local" >&2
  echo "doctor: delta the merge dropped. Advisory: nothing is auto-merged." >&2
}

report_unassessed_overrides() {
  local i
  echo "doctor: an override's drift cannot be assessed:" >&2
  for i in "${!UNASSESSED[@]}"; do
    echo "  ${UNASSESSED[$i]}" >&2
  done
  echo "doctor: an override nothing can compare is the same failure as not" >&2
  echo "doctor: detecting drift at all (#238)." >&2
}

# #260 — version equality is a check on the STAMP, not on the content.
#
# #238's detector catches an override that has fallen BEHIND. It cannot catch
# divergence AT THE SAME VERSION, and #63 was reintroduced a second time through
# that opening: CannObserv/cannabis.observer-wordpress' shipping-work-php
# override recorded `version: "1.1"` against a vendor also at 1.1 — the doctor
# exited 0, correctly by its own contract — while its Step 1 had replaced
# upstream's `bash "<SKILL_SCRIPTS>/pre-ship.sh"` with `bash scripts/pre-ship.sh`
# for all six scripts. The stamp was honest; the content diverged underneath it.
# `version:` records the vendor version last synced from, and the failure mode
# is not "someone forgot to bump it" — it is "someone synced from 1.1 and, in
# the same edit, replaced upstream text with something worse".
#
# Divergence is also the EXPECTED state: an override exists to differ, so a
# check cannot simply diff and warn on any difference. What can be checked is a
# vendor's own claim about which fragments are not optional. Upstream fences
# those in its SKILL.md:
#
#   <!-- skill:required -->
#   ```bash
#   …the fragment…
#   ```
#
# The marker arms the fenced block that FOLLOWS it, and only a fenced block —
# prose gets legitimately reworded, and a fence is exactly delimited, so a
# fragment check over prose would flag every honest edit. An override must carry
# each armed block; anything else in the file is the override's own business.
#
# Compared whitespace-insensitively, because reflowing a code block is not
# dropping it. Absent a fence anywhere in the vendor file, this contributes
# nothing and says nothing: an upstream that marks nothing required is making no
# claim, which is different from an override that satisfies every claim made.
#
# #265 — the marker may name the fragment:
#
#   <!-- skill:required id=skill-scripts -->
#
# because an override can omit a fragment ON PURPOSE and the check had no way to
# hear that. A CannObserv/cli override of using-git-worktrees ships no scripts/
# at all — the project fixes the worktree root and enforces it from its own
# script — so the <SKILL_SCRIPTS> resolution loop resolves nothing there. Both
# offered remedies made that file worse: pasting the fragment back in puts a
# fence that cannot succeed into a skill file, which is the #63 shape arriving
# through the remedy, and "drop the override for per-file symlinks" has nothing
# to apply to when the whole delta IS SKILL.md. Meanwhile the check was already
# satisfiable by dead text — it is a whole-file substring search, so pasting the
# block under a "not used here" heading silenced it. The declaration is the
# honest version of that move, and check_silent_forks' .skills/forked-ok is the
# same escape hatch one check away.
#
# The id is what keeps the declaration from becoming a blanket mute: a
# declaration names the ONE fragment it excuses, so a fragment armed in a later
# release still reports against a file that already carries one. An un-idded
# marker cannot be declared at all — its vendor adds an id first. Each line is
# "<id>\t<fragment>", tab-delimited because the fragment is whitespace-collapsed
# and so can contain no tab of its own.
# The one arming form, shared with malformed_markers below so the two cannot
# drift into disagreeing about what a marker is.
MARKER_RE='^[[:space:]]*<!--[[:space:]]*skill:required([[:space:]]+id=[A-Za-z0-9][A-Za-z0-9._-]*)?[[:space:]]*-->[[:space:]]*$'

# #265 CR round 2 — code-fence bookkeeping, shared by both scanners for the
# same reason MARKER_RE is: they answer the same question about the same file.
#
# Each tracked fences only far enough for its own job, and a marker inside a
# FENCED EXAMPLE was live text to both. A vendor SKILL.md documenting this very
# convention therefore armed a fragment nobody claimed — every override of it
# was told it had dropped the sample — and a "never write this" note showing the
# malformed form was reported as committing it. Both are the shape #260 CR round
# 1 already paid for once: the false positive landing on the most careful file
# there is.
#
# The fence is keyed on its FULL RUN, and closes on a run of the same character
# at least as long, alone on its line — CommonMark's rule, and the only one
# under which a ```` block quoting ``` is not cut short at the quote.
AWK_FENCE='
  function fence_run(line,   t) {
    t = line
    sub(/^[[:space:]]*/, "", t)
    if (match(t, /^`+/) || match(t, /^~+/)) return substr(t, RSTART, RLENGTH)
    return ""
  }
  # 3 = this line opened a fence, 2 = closed one, 1 = inside one, 0 = outside.
  function fence_scan(line,   run) {
    run = fence_run(line)
    if (incode) {
      if (run != "" \
          && substr(run, 1, 1) == substr(openfence, 1, 1) \
          && length(run) >= length(openfence) \
          && line ~ ("^[[:space:]]*" run "[[:space:]]*$")) {
        incode = 0
        return 2
      }
      return 1
    }
    if (length(run) >= 3) {
      incode = 1; openfence = run; openline = NR; opentext = line
      return 3
    }
    return 0
  }
'

required_fragments() {
  awk -v marker="$MARKER_RE" "$AWK_FENCE"'
    {
      state = fence_scan($0)
      # A fence opens: it is the armed block only if a marker armed before it.
      if (state == 3) { if (armed) { infence = 1; buf = "" } next }
      if (state == 2) {
        if (infence) {
          infence = 0; armed = 0
          gsub(/[[:space:]]+/, " ", buf)
          sub(/^ /, "", buf); sub(/ $/, "", buf)
          if (buf != "") printf "%s\t%s\n", fid, buf
          fid = ""
        }
        next
      }
      if (state == 1) { if (infence) buf = buf " " $0; next }
      # Outside every fence, which is the only place a marker is a claim rather
      # than an example. A second marker before any fence just re-arms.
      if ($0 ~ marker) {
        armed = 1
        fid = ""
        if (match($0, /id=[A-Za-z0-9][A-Za-z0-9._-]*/)) {
          fid = substr($0, RSTART + 3, RLENGTH - 3)
        }
        next
      }
      # An armed marker followed by prose rather than a fence is a malformed
      # mark, not a licence to claim the next fence further down the file.
      if (armed && $0 !~ /^[[:space:]]*$/) { armed = 0; fid = "" }
    }
  ' "$1"
}

# #265 CR round 1 — a marker that misses the arming form arms NOTHING, and the
# silence is total: the vendor's strongest claim about its own file degrades
# into a comment, and neither side is told. `id=` with no slug after it,
# `id=two words`, a stray attribute — the id syntax #265 added is the reason
# there are now more ways to write one wrong, and this repo's suite can only
# hold its OWN markers.
#
# Only a line OUTSIDE every code fence that OPENS with `<!--` counts as an
# attempt, so neither a SKILL.md discussing the convention mid-sentence nor one
# showing the wrong form inside a fenced example is accused of malforming one.
malformed_markers() {
  awk -v marker="$MARKER_RE" "$AWK_FENCE"'
    {
      if (fence_scan($0) != 0) next
      if ($0 ~ /^[[:space:]]*<!--/ && $0 ~ /skill:required/ && $0 !~ marker) {
        printf "%d\t%s\n", NR, $0
      }
    }
  ' "$1"
}

# #265 CR round 3 — a fence with no closing line runs to EOF, which is
# CommonMark's rule and therefore the right parse of a wrong file. What is not
# right is losing what that swallows in silence: in a vendor, every
# <!-- skill:required --> below the open fence is inert, so a guarantee is gone
# to a missing line and nothing says so; in an override, the mirror — prose
# below it is read as an instruction to execute.
#
# The END of the file is the only place this is DECIDABLE, and the check claims
# no more than that. A marker sitting inside a fence is textually identical
# whether it was quoted as an example (which #265 CR round 2 made deliberately
# inert) or swallowed by a fence someone failed to close mid-file; nothing can
# separate the two, and guessing would either re-arm every documented sample or
# accuse every honest one. A file that ENDS inside a fence is not ambiguous.
unclosed_fence() {
  awk "$AWK_FENCE"'
    { fence_scan($0) }
    END { if (incode) printf "%d\t%s\n", openline, opentext }
  ' "$1"
}

# The whole file as one whitespace-collapsed line, so a fragment match is
# insensitive to line wrapping and indentation and nothing else.
#
# Joined first and collapsed ONCE at the end, not per line. Collapsing each line
# before appending leaves an indented line contributing its own leading space
# next to the joining one, so every re-indented block in the override missed a
# fragment that was verbatim apart from its indentation — a whitespace-sensitive
# check wearing a whitespace-insensitive one's clothes.
flatten_md() {
  awk '
    { flat = flat " " $0 }
    END {
      gsub(/[[:space:]]+/, " ", flat)
      sub(/^ /, "", flat); sub(/ $/, "", flat)
      print flat
    }
  ' "$1"
}

# Output channels for the three content checks, at top scope for the same reason
# FORKED is: one remedy block for the whole list, not one per finding. The two
# drift voices have their own channels — DRIFTED and UNASSESSED, declared beside
# the reporters that drain them — so this block is three of five, not the list.
declare -a MISSING_FRAGMENT=()
declare -a MISSING_FRAGMENT_ID=()
declare -a MISSING_FRAGMENT_TEXT=()
declare -a BARE_SCRIPT_PATH=()
declare -a BARE_SCRIPT_UNRESOLVED=()
declare -a STALE_DECLARATION=()
declare -a MALFORMED_MARKER=()
declare -a UNCLOSED_FENCE=()
# Vendor files already scanned, so two overrides of one target report its
# markers once. Space-delimited, matched whole, like FORK_EXEMPT_NAMES.
MALFORMED_SEEN=" "

report_missing_fragments() {
  local i
  echo "doctor: an override omits text its vendor marks as required:" >&2
  for i in "${!MISSING_FRAGMENT[@]}"; do
    echo "  ${MISSING_FRAGMENT[$i]}" >&2
    echo "      missing (${MISSING_FRAGMENT_ID[$i]}): ${MISSING_FRAGMENT_TEXT[$i]}" >&2
  done
  echo "doctor: upstream fences these with <!-- skill:required id=… --> as" >&2
  echo "doctor: dropping one reintroduces a fixed failure. An override is" >&2
  echo "doctor: SUPPOSED to differ, so this is not a diff — it is the small" >&2
  echo "doctor: set the vendor says is not optional, and a version: stamp" >&2
  echo "doctor: cannot see it: #63 came back a second time under a version" >&2
  echo "doctor: that matched exactly (#260). Re-sync the fragment. If it" >&2
  echo "doctor: genuinely cannot apply here — the override ships none of the" >&2
  echo "doctor: scripts the block resolves, say — declare that rather than" >&2
  echo "doctor: pasting back a fence that cannot run (#265):" >&2
  echo "doctor:   metadata:" >&2
  echo "doctor:     omits-required: \"<id>: why it cannot apply here\"" >&2
  echo "doctor: naming the id printed above. A declaration excuses that one" >&2
  echo "doctor: fragment, so a newly armed one still reports; one printed as" >&2
  echo "doctor: 'no id' cannot be declared until its vendor names it." >&2
  echo "doctor: Advisory: nothing is changed for you." >&2
}

# #265 — a declaration that excuses nothing is worse than no declaration: it
# reads, to the next person, as a decision already taken. The two ways to get
# there are a vendor that renamed or dropped the fragment since the line was
# written, and an override that has since re-synced the fragment it excuses.
# Both leave a mute in the frontmatter with nothing under it.
report_stale_declarations() {
  local i
  echo "doctor: an override's omits-required: excuses nothing:" >&2
  for i in "${!STALE_DECLARATION[@]}"; do
    echo "  ${STALE_DECLARATION[$i]}" >&2
  done
  echo "doctor: the declaration names the id on a vendor's <!-- skill:required" >&2
  echo "doctor: id=<slug> --> marker, then the reason after a colon:" >&2
  echo "doctor:   omits-required: \"skill-scripts: this project ships none\"" >&2
  echo "doctor: A line matching no armed fragment still reads as a decision" >&2
  echo "doctor: taken while excusing nothing, and the next fragment the vendor" >&2
  echo "doctor: arms will not be covered by it either (#265). Correct the id," >&2
  echo "doctor: or drop the line. Advisory: nothing is changed for you." >&2
}

report_malformed_markers() {
  local i
  echo "doctor: a vendor marks a fragment required in a form that arms" >&2
  echo "doctor: nothing, so the claim is not checked against any override:" >&2
  for i in "${!MALFORMED_MARKER[@]}"; do
    echo "  ${MALFORMED_MARKER[$i]}" >&2
  done
  echo "doctor: write it exactly, naming the fragment:" >&2
  echo "doctor:   <!-- skill:required id=<slug> -->" >&2
  echo "doctor: The bare <!-- skill:required --> also arms, but a consumer for" >&2
  echo "doctor: whom that fragment cannot apply then has no way to declare it" >&2
  echo "doctor: — the id is what a declaration names (#265). Anything else is" >&2
  echo "doctor: a comment: the block it meant to arm is compared against" >&2
  echo "doctor: nothing, and nothing said so until now. The file is the" >&2
  echo "doctor: VENDOR'S — fix it upstream, or report it there; a consumer" >&2
  echo "doctor: cannot repair a claim it does not own. Advisory." >&2
}

report_unclosed_fences() {
  local i
  echo "doctor: a file ends inside a code fence, so everything below the" >&2
  echo "doctor: fence is read as code:" >&2
  for i in "${!UNCLOSED_FENCE[@]}"; do
    echo "  ${UNCLOSED_FENCE[$i]}" >&2
  done
  echo "doctor: in a vendor that makes every <!-- skill:required --> marker" >&2
  echo "doctor: below it inert — a guarantee lost to a missing line, and no" >&2
  echo "doctor: override is checked against it. In an override it is the" >&2
  echo "doctor: mirror: prose below the fence reads as an instruction to" >&2
  echo "doctor: execute. An unclosed fence runs to the end of the file, so" >&2
  echo "doctor: the parse is right and the file is wrong (#265): close the" >&2
  echo "doctor: fence. Advisory: nothing is changed for you." >&2
}

report_bare_script_paths() {
  local i
  echo "doctor: an override tells an agent to run a script by a path that" >&2
  echo "doctor: resolves from nowhere:" >&2
  for i in "${!BARE_SCRIPT_PATH[@]}"; do
    echo "  ${BARE_SCRIPT_PATH[$i]}" >&2
    # Only when the line carries more than one invocation: there the finding is
    # about a path the reader cannot pick out, and the exemption below reads as
    # if it had covered the whole line (#265 CR round 2).
    [ -z "${BARE_SCRIPT_UNRESOLVED[$i]}" ] ||
      echo "      unresolved: ${BARE_SCRIPT_UNRESOLVED[$i]}" >&2
  done
  echo "doctor: the agent's cwd is the PROJECT root, and a skill's scripts/" >&2
  echo "doctor: ships inside the skill — so 'bash scripts/X.sh' fails with" >&2
  echo "doctor: 'No such file or directory' (#63). A script that cd's to" >&2
  echo "doctor: 'git rev-parse --show-toplevel' does not fix this: that" >&2
  echo "doctor: resolves the root it OPERATES on, not the path bash uses to" >&2
  echo "doctor: OPEN the file. Use the resolved placeholder form instead:" >&2
  echo "doctor:   bash \"<SKILL_SCRIPTS>/X.sh\"" >&2
  echo "doctor: A path that EXISTS at the project root is the project's own" >&2
  echo "doctor: script, not the skill's, and is not listed here (#266)." >&2
  echo "doctor: The vendor's own suite gates this; nothing gated a consumer's" >&2
  echo "doctor: override until #260. Advisory: nothing is changed for you." >&2
}

# Both content checks for one override, run whatever the version stamps say —
# which is the point: the two occurrences of #63 differed only in whether the
# stamp had been kept honest, and the second one had.
check_override_content() {
  local dir="$1" target="$2" vendor_md="$3" md="$4"
  local flat line fid frag label decl ids reason declared armed excused tok
  local n rest paths text unresolved
  local invalid unknown unknown_n carried seen declared_n
  local -a toks=()
  local -a plist=()
  # Findings already on the channel from earlier overrides. A declaration that
  # is broken in more than one way is one finding, not a pile: the missing
  # warrant is only worth saying when the ids themselves are sound.
  local before="${#STALE_DECLARATION[@]}"

  # #265 — the override's declaration, parsed before the fragments it excuses.
  # Grammar: "<id>[, <id>…]: <why>". Ids FIRST, because frontmatter_value reads
  # a single line and a reason long enough to be worth writing gets folded
  # across two — the ids stay parseable, the tail of the prose is for people.
  # A declaration with no reason is honoured and reported: the ids are what the
  # check needs, and an unexplained mute is the thing that rots.
  declared=" "
  decl="$(frontmatter_value "$md" omits-required)"
  if [ -n "$decl" ]; then
    ids="${decl%%:*}"
    reason=""
    [ "$ids" != "$decl" ] && reason="${decl#*:}"
    reason="${reason#"${reason%%[![:space:]]*}"}"
    IFS=', ' read -r -a toks <<<"$ids"
    invalid=""
    for tok in ${toks[@]+"${toks[@]}"}; do
      [ -n "$tok" ] || continue
      # A fragment id is a slug. Validating rejects the shapes that would
      # otherwise be matched as one — a stray quote, a glob, half a sentence.
      case "$tok" in
        *[!A-Za-z0-9._-]*)
          invalid="${invalid:+$invalid, }$tok"
          continue
          ;;
      esac
      declared="$declared$tok "
    done
    # One line for the whole value, not one per token: a declaration written as
    # prose splits into a word per finding, and the reader learns to skim
    # (#265 CR round 1).
    if [ -n "$invalid" ]; then
      STALE_DECLARATION+=("$dir/SKILL.md: omits-required: names what is not a fragment id: $invalid")
    fi
    if [ "$declared" = " " ] && [ -z "$invalid" ]; then
      STALE_DECLARATION+=("$dir/SKILL.md: omits-required: \"$decl\" names no id before its colon")
    fi
  fi

  flat="$(flatten_md "$md")"
  armed=" "
  excused=" "
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    fid="${line%%$'\t'*}"
    frag="${line#*$'\t'}"
    [ -n "$frag" ] || continue
    [ -n "$fid" ] && armed="$armed$fid "
    # Quoted inside the pattern, so every glob character in the fragment is
    # matched literally — a required block is code, and code is full of them.
    case "$flat" in *"$frag"*) continue ;; esac
    if [ -n "$fid" ]; then
      case "$declared" in
        *" $fid "*) excused="$excused$fid "; continue ;;
      esac
    fi
    label="no id"
    [ -n "$fid" ] && label="id=$fid"
    MISSING_FRAGMENT+=("$dir/SKILL.md (overrides $target)")
    MISSING_FRAGMENT_ID+=("$label")
    MISSING_FRAGMENT_TEXT+=("$(printf '%.100s' "$frag")…")
  done <<EOF
$(required_fragments "$vendor_md")
EOF

  # A declared id that excused nothing — see report_stale_declarations. Checked
  # after the fragments rather than during, because "the override carries that
  # fragment" is only knowable once every armed block has been looked for.
  unknown=""
  unknown_n=0
  carried=""
  declared_n=0
  seen=" "
  for tok in ${toks[@]+"${toks[@]}"}; do
    [ -n "$tok" ] || continue
    case "$declared" in *" $tok "*) ;; *) continue ;; esac
    # A repeated id is one declaration, however many times it is written.
    case "$seen" in *" $tok "*) continue ;; esac
    seen="$seen$tok "
    declared_n=$((declared_n + 1))
    case "$armed" in
      *" $tok "*)
        case "$excused" in
          *" $tok "*) ;;
          *) carried="${carried:+$carried, }$tok" ;;
        esac
        ;;
      *) unknown="${unknown:+$unknown, }$tok"; unknown_n=$((unknown_n + 1)) ;;
    esac
  done
  if [ -n "$unknown" ]; then
    STALE_DECLARATION+=("$dir/SKILL.md: id=$unknown — the vendor arms no such fragment")
    # Nothing matched, and there is more than one token: the likeliest cause is
    # a reason written where the ids go, which splits into a word per id.
    if [ "$unknown_n" -eq "$declared_n" ] && [ "$unknown_n" -gt 1 ]; then
      STALE_DECLARATION+=("      (a reason written before the colon parses as ids — the grammar is \"<id>[, <id>…]: <why>\")")
    fi
  fi
  if [ -n "$carried" ]; then
    STALE_DECLARATION+=("$dir/SKILL.md: id=$carried — already carried by the override")
  fi
  if [ -n "$decl" ] && [ -z "$reason" ] && [ "$declared" != " " ] \
    && [ "${#STALE_DECLARATION[@]}" -eq "$before" ]; then
    STALE_DECLARATION+=("$dir/SKILL.md: omits-required: \"$decl\" carries no reason after the id")
  fi

  # The #63 shape itself, checked directly rather than only through a fence.
  # It costs the vendor nothing to mark, and this catches it in an override of
  # a skill whose upstream marks nothing at all — which is every vendor that
  # has not adopted the fence yet, and the state the report arrived from.
  #
  # INSIDE A FENCED CODE BLOCK ONLY, which is where both occurrences of #63
  # lived and where the string is an instruction to execute. A whole-file grep
  # flagged prose that WARNS against the pattern — an override carrying
  # upstream's own "never write `bash scripts/X.sh`" note was reported as
  # committing it. That lands the false positive on the most careful override
  # there is, and a brand-new advisory detector that cries wolf on its first
  # encounter is one operators learn to skim.
  #
  # #266 — and NOT when the named path exists at the project root, because
  # `scripts/` there is also where a consumer keeps its OWN scripts and the
  # check read both as the skill's. CannObserv/cli's override names
  # `scripts/setup-worktree.sh`, a project-owned script committed at that repo's
  # root, from a step that has just cd'd into a worktree — correct as written,
  # reported anyway, and with no correct substitution to offer, since
  # <SKILL_SCRIPTS> resolves to a skill directory and that override ships no
  # scripts/ at all. The doctor's cwd is the project root, so the
  # distinguishing fact is directly testable and precise in both directions: a
  # skill's scripts/X.sh does not exist at the project root, so #63's shape
  # still reports; a project's does, so a correct instruction stays quiet. It
  # also covers the copy the vendor already blesses — using-git-worktrees says
  # "a project-local scripts/ copy wins if one exists", which is a consumer copy
  # this check would have flagged the moment anyone spelled it out.
  #
  # It makes the check state-dependent — delete the project script later and the
  # line starts reporting — and that is the right signal rather than a cost: the
  # instruction really did break, and a declared exemption would have gone on
  # asserting a file that is no longer there.
  #
  # `N\tpaths\ttext` from awk, split here rather than glued into one string: the
  # fork and seam reports print copy-pasteable `path:line` locators, and
  # `path: N:` was neither.
  #
  # EVERY invocation on the line, not just the first (#265 CR round 1). A single
  # match per line meant the exemption was line-scoped where the instruction is
  # what it judges: `bash scripts/present.sh && bash scripts/absent.sh` was
  # decided entirely by `present.sh`, so a project-owned script at the head of a
  # line silently laundered a broken one behind it — a false negative this
  # exemption INTRODUCED, since the line reported unconditionally before it.
  # The line is reported once if any of its paths resolves nowhere.
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    n="${line%%$'\t'*}"
    rest="${line#*$'\t'}"
    paths="${rest%%$'\t'*}"
    text="${rest#*$'\t'}"
    # read -a rather than word splitting: a path is unquoted here and a fenced
    # `scripts/*.sh` would otherwise be expanded against the project tree.
    IFS=' ' read -r -a plist <<<"$paths"
    unresolved=""
    for path in ${plist[@]+"${plist[@]}"}; do
      [ -f "$path" ] || unresolved="${unresolved:+$unresolved, }$path"
    done
    [ -n "$unresolved" ] || continue
    BARE_SCRIPT_PATH+=("$dir/SKILL.md:$n  $text")
    # One invocation on the line is its own locator; naming it again would be
    # the same string twice.
    if [ "${#plist[@]}" -gt 1 ]; then
      BARE_SCRIPT_UNRESOLVED+=("$unresolved")
    else
      BARE_SCRIPT_UNRESOLVED+=("")
    fi
  done <<EOF
$(awk "$AWK_FENCE"'
    {
      # state 1 is INSIDE a fence — not its opening or closing line, and not
      # the prose around it. #265 CR round 3: this scanner kept a fence model
      # of its own, keyed on three characters, and a ```` block quoting an odd
      # number of ``` lines inverted the state for the rest of the file. That
      # reported prose WARNING against the pattern as committing it, which is
      # the false positive #260 CR round 1 removed, reachable again through the
      # one scanner round 2 did not convert. One model, three callers.
      if (fence_scan($0) != 1) next
      rest = $0; paths = ""
      while (match(rest, /bash[[:space:]]+scripts\/[^[:space:]]+\.sh/)) {
        p = substr(rest, RSTART, RLENGTH)
        sub(/^bash[[:space:]]+/, "", p)
        paths = paths " " p
        rest = substr(rest, RSTART + RLENGTH)
      }
      if (paths != "") {
        sub(/^ /, "", paths)
        printf "%d\t%s\t%s\n", NR, paths, $0
      }
    }
  ' "$md")
EOF

  # The vendor's own markers, reported against the vendor file because that is
  # where the repair belongs. Once per vendor file, however many overrides
  # target it.
  case "$MALFORMED_SEEN" in
    *" $vendor_md "*) ;;
    *)
      MALFORMED_SEEN="$MALFORMED_SEEN$vendor_md "
      while IFS= read -r line; do
        [ -n "$line" ] || continue
        MALFORMED_MARKER+=("$vendor_md:${line%%$'\t'*}  ${line#*$'\t'}")
      done <<EOF
$(malformed_markers "$vendor_md")
EOF
      while IFS= read -r line; do
        [ -n "$line" ] || continue
        UNCLOSED_FENCE+=("$vendor_md:${line%%$'\t'*}  ${line#*$'\t'}")
      done <<EOF
$(unclosed_fence "$vendor_md")
EOF
      ;;
  esac

  # The override too: an unclosed fence there is what makes the scan above read
  # the tail of the file as code.
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    UNCLOSED_FENCE+=("$md:${line%%$'\t'*}  ${line#*$'\t'}")
  done <<EOF
$(unclosed_fence "$md")
EOF
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
  DRIFTED=()
  UNASSESSED=()
  MISSING_FRAGMENT=()
  MISSING_FRAGMENT_ID=()
  MISSING_FRAGMENT_TEXT=()
  BARE_SCRIPT_PATH=()
  BARE_SCRIPT_UNRESOLVED=()
  STALE_DECLARATION=()
  MALFORMED_MARKER=()
  MALFORMED_SEEN=" "
  UNCLOSED_FENCE=()
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
      record_override_unassessed "$dir" "$target" \
        "no vendor copy at $vendor_md (uninitialized submodule, or the skill moved upstream)"
      continue
    fi

    # BEFORE the version comparisons, and outside every `continue` below them.
    # An override that matches the vendor's version exactly takes the first
    # `continue` in the block that follows, which is exactly the state #260
    # reports: the content check must not sit behind a verdict that says there
    # is nothing to look at.
    check_override_content "$dir" "$target" "$vendor_md" "$md"

    v_ver="$(frontmatter_value "$vendor_md" version)"
    o_ver="$(frontmatter_value "$md" version)"
    if [ -n "$v_ver" ]; then
      if [ -z "$o_ver" ]; then
        record_override_unassessed "$dir" "$target" \
          "the vendor is at version $v_ver and the override records no version: (the vendor version last synced from)"
      elif [ "$o_ver" != "$v_ver" ]; then
        record_override_drift "$dir" "$target" "version $o_ver" "version $v_ver"
      fi
      continue
    fi

    # Unversioned upstream: the synced-from fallback.
    synced="$(frontmatter_value "$md" synced-from)"
    if [ -z "$synced" ]; then
      record_override_unassessed "$dir" "$target" \
        "the vendor ships no version: and the override records no synced-from: (\"<repo> <tag> (<commit>)\")"
      continue
    fi
    rec="${synced##*(}"
    rec="${rec%%)*}"
    if [ "$rec" = "$synced" ] || [ -z "$rec" ]; then
      record_override_unassessed "$dir" "$target" \
        "synced-from: \"$synced\" carries no (commit) to compare against"
      continue
    fi
    if ! git -C "$repo_dir" rev-parse --verify --quiet "$rec^{commit}" >/dev/null 2>&1; then
      record_override_unassessed "$dir" "$target" \
        "the recorded commit $rec is not in the vendor's history (shallow clone?)"
      continue
    fi
    # diff --quiet: 0 unchanged, 1 changed, anything else is an error — which
    # must land in "cannot be assessed" rather than in either verdict.
    rc=0
    git -C "$repo_dir" diff --quiet "$rec" HEAD -- "$skill_rel" 2>/dev/null || rc=$?
    if [ "$rc" -eq 1 ]; then
      record_override_drift "$dir" "$target" "commit $rec" "a vendor tree that has since changed $skill_rel"
    elif [ "$rc" -ne 0 ]; then
      record_override_unassessed "$dir" "$target" \
        "'git diff $rec HEAD -- $skill_rel' failed in $repo_dir"
    fi
  done
  # After the loop, so a repo with several overrides gets one remedy block per
  # class rather than one per file — the shape check_silent_forks settled on.
  [ "${#DRIFTED[@]}" -eq 0 ] || report_drifted_overrides
  [ "${#UNASSESSED[@]}" -eq 0 ] || report_unassessed_overrides
  [ "${#MISSING_FRAGMENT[@]}" -eq 0 ] || report_missing_fragments
  [ "${#STALE_DECLARATION[@]}" -eq 0 ] || report_stale_declarations
  [ "${#MALFORMED_MARKER[@]}" -eq 0 ] || report_malformed_markers
  [ "${#UNCLOSED_FENCE[@]}" -eq 0 ] || report_unclosed_fences
  [ "${#BARE_SCRIPT_PATH[@]}" -eq 0 ] || report_bare_script_paths
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
