#!/usr/bin/env bash
# Wrapper for running dbt locally with credentials from secrets/ files.
# Usage: ./scripts/dbt.sh debug|run|test|... [dbt flags]
#
# Credentials: uses POSTGRES_USER/POSTGRES_PASSWORD env vars if set,
# otherwise reads from secrets/ files (same files as Docker Compose).
# Forwards all arguments to dbt with the local profile target.

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <command> [dbt flags]"
    echo ""
    echo "Commands: debug, run, test, build, compile, clean, deps"
    echo "Example: $0 run --select gold"
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Load environment variables (.env for shared defaults, .env.local for local overrides)
set -a
# shellcheck source=/dev/null
[ -f "$REPO_ROOT/.env" ] && . "$REPO_ROOT/.env"
# shellcheck source=/dev/null
[ -f "$REPO_ROOT/.env.local" ] && . "$REPO_ROOT/.env.local"
set +a

POSTGRES_USER="${POSTGRES_USER:-$(cat "$REPO_ROOT/secrets/postgres_root_username")}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-$(cat "$REPO_ROOT/secrets/postgres_root_password")}"
export POSTGRES_USER POSTGRES_PASSWORD

exec uv run dbt "$@" \
    --project-dir "$REPO_ROOT/dbt" \
    --profiles-dir "$REPO_ROOT/dbt" \
    --target local
