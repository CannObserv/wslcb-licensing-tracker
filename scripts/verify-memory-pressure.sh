#!/usr/bin/env bash
# Verify the #175 memory-pressure stack is actually in force on this host.
#
# Every expectation is read from infra/ rather than hardcoded, so this cannot
# drift from the committed config. tests/test_infra_memory_pressure.py checks
# that those files are coherent with each other; this checks that the running
# host matches them. Both are needed: the whole point of #175 is that a
# reservation can read as configured while delivering nothing.
#
# Usage: scripts/verify-memory-pressure.sh [--quiet]
# Exit:  0 all good · 1 a check failed · 2 could not run a check
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INFRA="$ROOT/infra"
UNIT="$INFRA/wslcb-web.service"
SLICE="$INFRA/system.slice.d-10-wslcb-memory.conf"
EARLYOOM="$INFRA/default-earlyoom"
SYSCTL="$INFRA/sysctl.d-60-wslcb-memory.conf"

QUIET=0
[ "${1:-}" = "--quiet" ] && QUIET=1

RC=0
pass() { [ "$QUIET" -eq 1 ] || printf '  \033[32m✓\033[0m %s\n' "$1"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$1" >&2; RC=1; }
blocked() { printf '  \033[33m?\033[0m %s\n' "$1" >&2; [ "$RC" -eq 0 ] && RC=2; }

# Read a `key=value` directive from a systemd/env file, ignoring comments.
# Tolerates `key=value` and `key = value` alike: infra/ carries both styles
# (systemd units omit the spaces, sysctl.d conventionally includes them), and a
# parser that silently matched only one would report an empty expectation.
directive() {
  awk -v k="$2" -F= '
    /^[[:space:]]*#/ {next}
    {
      key=$1; sub(/^[[:space:]]+/,"",key); sub(/[[:space:]]+$/,"",key)
      if (key!=k) next
      sub(/^[^=]*=/,""); sub(/^[[:space:]]+/,""); sub(/[[:space:]]+$/,"")
      print; exit
    }' "$1"
}

# systemd size suffixes are powers of 1024, not 1000 (systemd.syntax(7)):
# MemoryLow=256M lands as 268435456 in memory.low, not 256000000. Getting this
# wrong understates the expectation and lets a short parent grant pass.
size_bytes() {
  local v="$1" n u
  n="${v%[KMG]}"; u="${v#"$n"}"
  case "$u" in K) echo $((n*1024)) ;; M) echo $((n*1024*1024)) ;; G) echo $((n*1024*1024*1024)) ;; *) echo "$n" ;; esac
}

[ "$QUIET" -eq 1 ] || echo "Memory-pressure stack (#175) — expectations read from infra/"

# --- 1. Effective cgroup protection -----------------------------------------
# The reservation that matters is min(unit, parent): cgroup v2 caps a cgroup's
# effective low at its ancestors'. A unit value alone proves nothing (CR 1).
want_unit=$(size_bytes "$(directive "$UNIT" MemoryLow)")
u_low=$(cat /sys/fs/cgroup/system.slice/wslcb-web.service/memory.low 2>/dev/null)
p_low=$(cat /sys/fs/cgroup/system.slice/memory.low 2>/dev/null)
if [ -z "$u_low" ] || [ -z "$p_low" ]; then
  blocked "cgroup memory.low unreadable — is wslcb-web.service running?"
else
  eff=$(( u_low < p_low ? u_low : p_low ))
  if [ "$eff" -ge "$want_unit" ]; then
    pass "effective memory protection $((eff/1024/1024)) MiB (unit $((u_low/1024/1024)), parent $((p_low/1024/1024)))"
  else
    fail "effective protection $((eff/1024/1024)) MiB < the $((want_unit/1024/1024)) MiB the unit reserves — parent slice grants $((p_low/1024/1024)) MiB (install $SLICE)"
  fi
fi

# --- 2. OOM ranking ----------------------------------------------------------
want_adj=$(directive "$UNIT" OOMScoreAdjust)
main=$(systemctl show -p MainPID --value wslcb-web.service 2>/dev/null)
# NB: never pgrep -f here — the pattern matches this script's own command line.
if [ -z "$main" ] || [ "$main" = 0 ]; then
  blocked "wslcb-web.service has no MainPID — not running?"
else
  have_adj=$(cat "/proc/$main/oom_score_adj" 2>/dev/null)
  if [ "$have_adj" = "$want_adj" ]; then
    pass "oom_score_adj $have_adj (score $(cat "/proc/$main/oom_score" 2>/dev/null)), matches $(basename "$UNIT")"
  else
    fail "oom_score_adj is $have_adj, $(basename "$UNIT") declares $want_adj — restart the unit"
  fi
fi

# --- 3. Kernel atomic-allocation reserve -------------------------------------
want_mfk=$(directive "$SYSCTL" vm.min_free_kbytes)
have_mfk=$(sysctl -n vm.min_free_kbytes 2>/dev/null)
if [ "$have_mfk" = "$want_mfk" ]; then
  pass "vm.min_free_kbytes $have_mfk"
else
  fail "vm.min_free_kbytes is $have_mfk, expected $want_mfk — sudo sysctl --system"
fi

# --- 4. earlyoom ------------------------------------------------------------
# Unit state is not evidence: apt starts the daemon before the config exists and
# `enable --now` will not restart it, so it can report active+enabled while
# running Debian's stock args (CR 9). The journal is the only place it says what
# it parsed.
args=$(directive "$EARLYOOM" EARLYOOM_ARGS | sed 's/^"//; s/"$//')
want_prefer=$(printf '%s' "$args" | grep -oE -- "--prefer +'[^']*'" | sed "s/.*'\(.*\)'/\1/")
want_avoid=$(printf '%s' "$args" | grep -oE -- "--avoid +'[^']*'" | sed "s/.*'\(.*\)'/\1/")
want_term=$(printf '%s' "$args" | grep -oE -- '-m +[0-9]+' | grep -oE '[0-9]+$')
if ! systemctl is-active --quiet earlyoom; then
  fail "earlyoom is not active"
elif ! systemctl is-enabled --quiet earlyoom 2>/dev/null; then
  fail "earlyoom is active but not enabled — it will not survive a reboot"
else
  jrn=$(journalctl -u earlyoom -b --no-pager 2>/dev/null)
  if [ -z "$jrn" ]; then
    blocked "earlyoom journal unreadable (need journal access) — cannot confirm parsed args"
  else
    start=$(printf '%s' "$jrn" | grep -n 'earlyoom v' | tail -1 | cut -d: -f1)
    jrn=$(printf '%s' "$jrn" | tail -n +"${start:-1}")
    ok=1
    printf '%s' "$jrn" | grep -qF "regex '$want_prefer'" || { fail "earlyoom --prefer regex not the configured '$want_prefer' — restart earlyoom"; ok=0; }
    printf '%s' "$jrn" | grep -qF "regex '$want_avoid'"  || { fail "earlyoom --avoid regex not the configured '$want_avoid' — restart earlyoom"; ok=0; }
    printf '%s' "$jrn" | grep -qE "SIGTERM when mem <= +${want_term}\.00%" || { fail "earlyoom SIGTERM threshold is not ${want_term}% — stock config still loaded? restart earlyoom"; ok=0; }
    [ "$ok" -eq 1 ] && pass "earlyoom active+enabled, journal confirms both regexes and the ${want_term}% threshold"
  fi
fi

# --- 4b. Who earlyoom would take right now ----------------------------------
# Config parsing proves nothing about the ranking (#178): earlyoom 1.7 applies
# --prefer (+300) and --avoid (-300), then silently drops every -1000 process,
# which is everything an agent session launches. Replay that over /proc and
# fail if the winner is something --avoid exists to protect.
if [ -n "$want_prefer" ] && [ -n "$want_avoid" ]; then
  top=$(for p in /proc/[0-9]*; do
          a=$(cat "$p/oom_score_adj" 2>/dev/null) || continue
          [ "$a" = -1000 ] && continue
          s=$(cat "$p/oom_score" 2>/dev/null) || continue
          c=$(cat "$p/comm" 2>/dev/null) || continue
          [[ $c =~ $want_prefer ]] && s=$((s + 300))
          [[ $c =~ $want_avoid ]] && s=$((s - 300))
          printf '%s\t%s\t%s\n' "$s" "${p#/proc/}" "$c"
        done | sort -rn | head -3)
  first=$(printf '%s\n' "$top" | head -1 | cut -f3)
  summary=$(printf '%s\n' "$top" | awk -F'\t' '{printf "%s%s(%s)", (NR>1?", ":""), $3, $1}')
  if [ -z "$first" ]; then
    blocked "no killable process readable in /proc"
  elif [[ $first =~ $want_avoid ]]; then
    fail "earlyoom's next victim would be '$first', which --avoid protects — top 3: $summary"
  else
    pass "earlyoom's next victims: $summary"
  fi
fi

# --- 5. SocratiCode is pinned, not installing at launch ----------------------
drv="$ROOT/skills-vendor/gregoryfoster-skills/skills/init-socraticode/scripts/mcp-driver.mjs"
if [ ! -f "$drv" ]; then
  blocked "mcp-driver.mjs not found — skills-vendor/ submodule not checked out?"
else
  src=$(node "$drv" resolve 2>/dev/null | sed -n 's/.*"source": "\([^"]*\)".*/\1/p')
  case "$src" in
    pinned*) pass "SocratiCode resolves to the $src" ;;
    "")      blocked "mcp-driver.mjs resolve produced no source" ;;
    *)       fail "SocratiCode resolves to '$src' — it installs at launch; see docs/DEPLOYMENT.md" ;;
  esac
fi

[ "$QUIET" -eq 1 ] || { echo; case $RC in
  0) echo "All checks passed — the stack is in force." ;;
  1) echo "FAILED — see above; docs/DEPLOYMENT.md 'Memory pressure'." >&2 ;;
  2) echo "INCOMPLETE — a check could not run; nothing was disproven." >&2 ;;
esac; }
exit $RC
