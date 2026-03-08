#!/usr/bin/env bash
# doc-check.sh — surfaces documentation drift before shipping
# Prints a quick checklist: what changed, what README/AGENTS.md sections to verify.
set -euo pipefail

README="README.md"
AGENTS="AGENTS.md"

echo "=== Doc-check: changed files (vs. origin/main) ==="
git diff --name-only origin/main...HEAD 2>/dev/null || git diff --name-only HEAD~1 HEAD

echo ""
echo "=== README.md — verify these sections if relevant files changed ==="

# New .py modules
NEW_PY=$(git diff --name-only --diff-filter=A origin/main...HEAD 2>/dev/null | grep '\.py$' || true)
if [ -n "$NEW_PY" ]; then
  echo "  [!] New .py files added — check README Project Structure tree:"
  echo "$NEW_PY" | sed 's/^/      /'
fi

# Deleted .py modules
DEL_PY=$(git diff --name-only --diff-filter=D origin/main...HEAD 2>/dev/null | grep '\.py$' || true)
if [ -n "$DEL_PY" ]; then
  echo "  [!] .py files removed — check README Project Structure tree:"
  echo "$DEL_PY" | sed 's/^/      /'
fi

# New/deleted templates
TMPL=$(git diff --name-only --diff-filter=AD origin/main...HEAD 2>/dev/null | grep '^templates/' || true)
if [ -n "$TMPL" ]; then
  echo "  [!] Template changes — check README Project Structure tree:"
  echo "$TMPL" | sed 's/^/      /'
fi

# New systemd unit files
SVC=$(git diff --name-only --diff-filter=A origin/main...HEAD 2>/dev/null | grep -E '\.(service|timer)$' || true)
if [ -n "$SVC" ]; then
  echo "  [!] New systemd units — check README Project Structure + Systemd setup:"
  echo "$SVC" | sed 's/^/      /'
fi

# Route changes (app.py, api_routes.py, admin_routes.py)
ROUTE_FILES=$(git diff --name-only origin/main...HEAD 2>/dev/null | grep -E '(app|api_routes|admin_routes)\.py' || true)
if [ -n "$ROUTE_FILES" ]; then
  echo "  [!] Route files changed — check README API Endpoints section:"
  echo "$ROUTE_FILES" | sed 's/^/      /'
fi

# seed_code_map.json changes
if git diff --name-only origin/main...HEAD 2>/dev/null | grep -q 'seed_code_map.json'; then
  COUNT=$(python3 -c "import json; print(len(json.load(open('seed_code_map.json'))))" 2>/dev/null || echo '?')
  echo "  [!] seed_code_map.json changed — verify README states correct entry count (currently: $COUNT)"
fi

echo ""
echo "=== AGENTS.md — verify Key Files and relevant sections ==="
NEW_ALL=$(git diff --name-only --diff-filter=A origin/main...HEAD 2>/dev/null | grep '\.py$' || true)
if [ -n "$NEW_ALL" ]; then
  echo "  [!] New modules — check AGENTS.md Key Files table:"
  echo "$NEW_ALL" | sed 's/^/      /'
fi

SCHEMA=$(git diff --name-only origin/main...HEAD 2>/dev/null | grep 'schema\.py' || true)
if [ -n "$SCHEMA" ]; then
  echo "  [!] schema.py changed — check AGENTS.md Schema section and Key Files"
fi

CLI=$(git diff --name-only origin/main...HEAD 2>/dev/null | grep 'cli\.py' || true)
if [ -n "$CLI" ]; then
  echo "  [!] cli.py changed — check AGENTS.md Common Tasks"
fi

echo ""
echo "=== All clear? Proceed to Step 2. ==="
