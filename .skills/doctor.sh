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
VERSION="2026-08-18-2"

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
  --check-only    Report broken symlinks and uninitialized skills-vendor/
                  submodules but make no changes: no submodule init, no
                  self-sync. Exits 1 for either, EXCEPT a submodule
                  .gitmodules holds with 'update = none' — the exit code
                  covers everything this mode reports as damage. (The
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
     pre-flight SSH check failed.
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

# The auto-refresh hook's contract is TWO artifacts — the symlink and the
# SessionStart registration in .claude/settings.json — and only the second one
# makes it run. A repo carrying the symlink without the registration looks
# installed to anyone who lists .claude/hooks/ and refreshes nothing, so its
# vendored skills freeze at whatever commit they were on.
#
# Four of twelve audited consumers were in exactly that state, pinned at one
# commit for over a week while the rest of the cohort moved through four skill
# versions (#167). Nothing detected it, because a half-installed hook is silent
# by construction: the missing half is the half that would have run.
#
# Reported HERE, for the same reason sync_self lives here — the doctor is the
# one code path that still runs in a repo whose refresh hook does not, whether
# through a reviewing-*/shipping-* preflight or a SessionStart entry of its
# own. Warn only; a wiring gap is not a dangling symlink and must not change
# this script's exit code, which Phase 1 preflights gate on with `|| exit 1`.
#
# Only when the symlink is present: that is what distinguishes "somebody
# installed this and it half-landed" from "this consumer never wanted the
# hook", and nagging the second group trains everyone to ignore the first.
check_refresh_registration() {
  local hook=".claude/hooks/skills-submodule-update.sh"
  local settings=".claude/settings.json"
  [ -L "$hook" ] || return 0
  # NOT `[ -f "$settings" ] || return 0`. No settings.json at all is the
  # strongest form of unregistered, so it must fall through to the warning
  # rather than out of the function — an early return there made the doctor
  # silent on the plainest half-install there is.
  #
  # Scoped to .hooks.SessionStart[].hooks[].command, not a grep over the file.
  # The basename appears in settings.json for reasons that are not
  # registrations — a `permissions.allow` entry naming the hook is the common
  # one — and a whole-file grep counted those, so this warning stayed silent on
  # exactly the half-installed repos it was added for (CR finding 1).
  #
  # No jq, no warning. The doctor is advisory and runs on every session start,
  # so a wrong warning is worse than none: it would fire in every consumer
  # without jq, including correctly-installed ones, and train the reader to
  # ignore the message. install-refresh.sh --check reports UNKNOWN in that case,
  # which is the right place for a demand that jq be installed.
  command -v jq >/dev/null 2>&1 || return 0
  if [ -f "$settings" ] && jq -e '[.hooks.SessionStart[]?.hooks[]?.command // ""]
            | any(contains("skills-submodule-update.sh"))' \
       "$settings" >/dev/null 2>&1; then
    return 0
  fi
  echo "doctor: $hook is installed but $settings does not register it," >&2
  echo "doctor: so the auto-refresh hook never runs and this repo's vendored" >&2
  echo "doctor: skills stay frozen at their current commit. Repair with:" >&2
  # Resolved here rather than printed as a glob. `bash skills-vendor/*/…` passes
  # every extra match as an argument to the first, and install-refresh.sh
  # rejects unknown arguments — so the paste-under-pressure path would fail on
  # any repo vendoring a second skills repo.
  local installer
  for installer in skills-vendor/*/skills/managing-skills/scripts/install-refresh.sh; do
    [ -f "$installer" ] || continue
    echo "doctor:   bash $installer" >&2
    return 0
  done
  echo "doctor:   bash <vendor>/skills/managing-skills/scripts/install-refresh.sh" >&2
  return 0
}

check_refresh_registration

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
scan_broken() {
  BROKEN=()
  local dir entry
  for dir in "${SCAN_DIRS[@]}"; do
    [ -d "$dir" ] || continue
    for entry in "$dir"/*; do
      [ -L "$entry" ] || continue
      if [ ! -e "$entry" ]; then
        BROKEN+=("$entry")
      fi
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

[ "$VERBOSE" = "1" ] && echo "doctor: self-healed; all scanned symlinks resolve" >&2
exit 0
