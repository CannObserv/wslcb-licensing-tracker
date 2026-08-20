#!/usr/bin/env bash
# pre-ship.sh — wslcb-licensing-tracker env-loading wrapper around the vendored
# shipping-work-python-fastapi gate.
#
# Why this exists: this repo's PostgreSQL tests read live secrets from the
# repo-root .env (TEST_DATABASE_URL and friends). Without them loaded, ~160 PG
# tests silently SKIP rather than fail, so the ship gate would go green on a
# suite that never ran. Everything else — every gate below the delegate — is
# upstream's and is deliberately NOT forked.
#
# Shape follows the wrapper contract documented in the vendored pre-ship.sh
# ("Project-local env loading (optional override point)"), with one deliberate
# deviation noted below.
#
# DEVIATION from that contract: it says to delegate through the `skills/...`
# symlink rather than `skills-vendor/...`. That assumes the wrapper sits at the
# project's own `scripts/pre-ship.sh`, leaving `skills/<name>/` free to be the
# vendor symlink. That layout does not work here: the skill's Step 1 resolution
# loop probes ONE directory using pre-ship.sh and reuses it for every later
# step, so a wrapper in `scripts/` resolves SKILL_SCRIPTS=scripts and then
# push.sh / check-status.sh / comment-issue.sh / close-issue.sh are all missing
# (this repo's `scripts/` carries only its own tooling plus a doc-check fork).
# Keeping the wrapper inside the skill's own scripts/ dir is what makes all six
# steps resolve — and it means the `skills/` path is occupied by this file, so
# the delegate has to be the vendor path. It is globbed, not hand-substituted,
# so a renamed vendor directory does not silently produce a plausible-but-dead
# path.
set -euo pipefail

PROJECT_ROOT=$(git rev-parse --show-toplevel)
cd "$PROJECT_ROOT"

DELEGATE=""
for candidate in skills-vendor/*/skills/shipping-work-python-fastapi/scripts/pre-ship.sh; do
  [[ -f "$candidate" ]] && { DELEGATE="$candidate"; break; }
done
[[ -n "$DELEGATE" ]] || {
  echo "ERROR: vendored gate missing under skills-vendor/*/skills/shipping-work-python-fastapi/scripts/pre-ship.sh" >&2
  echo "       fix: git submodule update --init --recursive" >&2
  exit 2
}

# Parse, never source. `set -a; . "$ENV"` executes the file as shell — on a
# secrets file that is arbitrary code execution, and this repo's .env holds
# GitHub PATs. `export $(cat … | xargs)` is worse still: it word-splits
# `K=two words`, chokes on `#` comments under set -e, and degenerates to a bare
# `export` when the file is absent, dumping every exported variable — secrets
# included — into the ship-gate transcript. A malformed line is skipped rather
# than fatal: a bad line in a secrets file must not decide whether the gate runs.
load_env() {
  # Declared local so the function is safe to reuse from an interactive shell
  # or another script — upstream's snippet omits this and clobbers three very
  # common variable names in its caller
  # (gregoryfoster/skills#211). Reuse it by lifting it out of this file rather
  # than re-deriving the parser:
  #   eval "$(sed -n '/^load_env() {/,/^}/p' <this file>)"; load_env "$PWD/.env"
  local line key val
  [ -r "$1" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line#"${line%%[![:space:]]*}"}        # drop leading blanks
    case $line in ''|\#*) continue ;; esac        # blank or comment
    line=${line#export }                          # tolerate `export K=v`
    case $line in *=*) ;; *) continue ;; esac
    key=${line%%=*} val=${line#*=}
    key=${key%"${key##*[![:space:]]}"}
    case $key in ''|*[!A-Za-z0-9_]*) continue ;; esac
    case $val in                                  # strip matched quotes
      \"*\") val=${val#\"} val=${val%\"} ;;
      \'*\') val=${val#\'} val=${val%\'} ;;
    esac
    export "$key=$val"
  done < "$1"
}

load_env "$PROJECT_ROOT/.env"

# exec so the exit code the Iron Law gates on propagates unchanged; "$@" so
# --help still reaches the delegate.
exec bash "$DELEGATE" "$@"
