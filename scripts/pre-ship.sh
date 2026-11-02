#!/usr/bin/env bash
# pre-ship.sh — wslcb-licensing-tracker env-loading wrapper around the vendored
# shipping-work gate.
#
# Why this exists: this repo's PostgreSQL tests read live secrets from the
# repo-root .env (TEST_DATABASE_URL and friends). Without them loaded, ~160 PG
# tests silently SKIP rather than fail, so the ship gate would go green on a
# suite that never ran. Everything else — every gate below the delegate — is
# upstream's and is deliberately NOT forked.
#
# Shape follows the wrapper recipe in the vendored pre-ship.sh ("Project-local
# env loading (optional override point)"). SKILL.md Step 1 resolves a repo-root
# scripts/pre-ship.sh for this script alone; the other five steps still find
# theirs in the skill (gregoryfoster/skills#301, #240).
#
# DEVIATION (#235): the recipe also loads `/etc/<project>/.env`. Not here:
# /etc/wslcb-licensing-tracker/.env is group-readable by this user and holds
# the PRODUCTION DATABASE_URL, which a test gate must never carry.
set -euo pipefail

PROJECT_ROOT=$(git rev-parse --show-toplevel)
cd "$PROJECT_ROOT"

# Through the skills/ symlink, never skills-vendor/: the symlink is the stable
# interface, the vendor layout is not.
DELEGATE="skills/shipping-work/scripts/pre-ship.sh"
[[ -f "$DELEGATE" ]] || {
  echo "ERROR: vendored gate missing at $DELEGATE" >&2
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
    case $key in ''|[0-9]*|*[!A-Za-z0-9_]*) continue ;; esac
    case $val in                                  # strip matched quotes
      \"*\") val=${val#\"} val=${val%\"} ;;
      \'*\') val=${val#\'} val=${val%\'} ;;
    esac
    export "$key=$val"
    PRE_SHIP_ENV_NAMES="${PRE_SHIP_ENV_NAMES:+$PRE_SHIP_ENV_NAMES }$key"
  done < "$1"
}

# Names only, never values: the vendored gate folds a hash of them into its
# per-SHA stamp, and writes none when the list is empty, so a run that loaded
# no env can't bless the commit for the run that does (skills#497).
export PRE_SHIP_ENV_NAMES=""

# A linked worktree has no .env (it's gitignored), so fall back to the primary
# checkout's: --git-common-dir names the primary's .git from any worktree.
# Without the fallback the PG tests skipped and the gate passed (#235).
LOCAL_ENV="$PROJECT_ROOT/.env"
ENV_FILE=$LOCAL_ENV
[ -r "$ENV_FILE" ] ||
  ENV_FILE="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/.env"
[ -r "$ENV_FILE" ] || {
  [ "$ENV_FILE" = "$LOCAL_ENV" ] || LOCAL_ENV="$LOCAL_ENV or $ENV_FILE"
  echo "WARN: no env file at $LOCAL_ENV;" >&2
  echo "      tests that need one will skip, and no stamp is written" >&2
}
load_env "$ENV_FILE"

# exec so the exit code the Iron Law gates on propagates unchanged; "$@" so
# --help still reaches the delegate.
exec bash "$DELEGATE" "$@"
