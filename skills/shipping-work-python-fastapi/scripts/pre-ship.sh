#!/usr/bin/env bash
# pre-ship.sh — wslcb-specific wrapper around the upstream
# shipping-work-python-fastapi/scripts/pre-ship.sh. Sources $PROJECT_ROOT/.env
# before delegating so test fixtures that read live secrets work.
set -euo pipefail
PROJECT_ROOT=$(git rev-parse --show-toplevel)
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a; source "$PROJECT_ROOT/.env"; set +a
fi
exec bash "$PROJECT_ROOT/skills-vendor/gregoryfoster-skills/skills/shipping-work-python-fastapi/scripts/pre-ship.sh" "$@"
