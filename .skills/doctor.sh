#!/usr/bin/env bash
# managing-skills-doctor: do not remove this marker — install-doctor.sh greps it
# doctor.sh — diagnose and self-heal dangling skill symlinks.
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
# vendor chain is broken. It walks skills/* symlinks, attempts a
# `git submodule update --init --recursive` if any dangle, and prints a
# clear actionable error if self-healing fails.
#
# Designed for use as a Phase 1 preflight in every reviewing-* / shipping-*
# SKILL.md invocation:
#
#   bash .skills/doctor.sh
#   bash scripts/gather-context.sh
#
# Usage: bash .skills/doctor.sh [--check-only] [--verbose] [--no-preflight] [--help]
set -euo pipefail

VERSION="2026-05-28-6"

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

Diagnose and self-heal dangling skill symlinks in skills/.

If any skills/<name> symlink does not resolve and a .git directory is
present, runs 'git submodule update --init --recursive' and re-checks.
Exits 0 silently when healthy. Exits non-zero with an actionable error
when self-healing fails or is not possible (e.g. no .git directory).

When submodule init fails with a well-known SSH/HTTPS auth signature
(Permission denied, Could not read from remote repository, Authentication
failed for 'https://'), a targeted remediation block is printed instead
of the generic 'submodule update failed' line. The same block is printed
by the pre-flight SSH check when .gitmodules references SSH remotes and
the agent isn't reachable from this shell. A separate remediation block
covers host-key-verification failures (ssh-keyscan-based fix).

Options:
  --check-only    Report broken symlinks but do not run submodule init
                  (overridden by the archive-checkout path when .git is
                  absent — the archive case prints its own diagnosis).
  --no-preflight  Skip the SSH pre-flight ping. Useful when the operator
                  knows the agent state and doesn't want the 3-second
                  ConnectTimeout on every invocation.
  --verbose, -v   Print resolution details even when healthy.
  --version       Print script version and exit.
  --help, -h      Show this help and exit.

Exit codes:
  0  All skill symlinks resolve (or skills/ does not exist).
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

# Nothing to check if the consumer doesn't use the skills/ pattern.
if [ ! -d skills ]; then
  [ "$VERBOSE" = "1" ] && echo "doctor: no skills/ directory — nothing to check" >&2
  exit 0
fi

# BROKEN is the output channel of scan_broken — declared at top scope so the
# function's communication pattern is visible without reading every call
# site. Using an array (rather than a single string) preserves
# paths-with-spaces correctly when later expanded as "${BROKEN[@]}".
declare -a BROKEN=()

# Walks skills/* and populates BROKEN with any dangling symlinks. A symlink
# is "broken" when it exists but its target does not resolve. Local
# overrides (regular directories) are skipped — they're not symlinks.
scan_broken() {
  BROKEN=()
  local entry
  for entry in skills/*; do
    [ -L "$entry" ] || continue
    if [ ! -e "$entry" ]; then
      BROKEN+=("$entry")
    fi
  done
}

scan_broken

if [ "${#BROKEN[@]}" -eq 0 ]; then
  [ "$VERBOSE" = "1" ] && echo "doctor: all skill symlinks resolve" >&2
  exit 0
fi

# At least one dangling symlink. Distinguish archive-checkout (no .git) from
# the normal git-submodule case before either self-healing or reporting,
# so --check-only never suggests a `git submodule` command in a checkout
# that doesn't have a .git dir.
if [ ! -d .git ] && [ ! -f .git ]; then
  echo "doctor: dangling skill symlinks detected and no .git directory present:" >&2
  printf '  %s\n' "${BROKEN[@]}" >&2
  echo "" >&2
  echo "This checkout was likely created from a source archive (zip/tarball)" >&2
  echo "rather than 'git clone'. The submodule pattern this repo uses is not" >&2
  echo "compatible with archive downloads. Clone with --recurse-submodules" >&2
  echo "instead, or vendor the skill scripts manually." >&2
  exit 1
fi

if [ "$CHECK_ONLY" = "1" ]; then
  echo "doctor: dangling skill symlinks detected:" >&2
  printf '  %s\n' "${BROKEN[@]}" >&2
  echo "Run 'git submodule update --init --recursive' to repair." >&2
  exit 1
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

echo "doctor: dangling skill symlinks detected — initializing submodules..." >&2

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

[ "$VERBOSE" = "1" ] && echo "doctor: self-healed; all skill symlinks resolve" >&2
exit 0
