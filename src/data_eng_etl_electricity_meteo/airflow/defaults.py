"""Shared Airflow defaults for all DAG factories.

Centralises production settings (retries, timeouts, start date) so that
``ingest_factory`` and ``load_pg_factory`` stay in sync.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

# ---------------------------------------------------------------------------
# DAG start date
# ---------------------------------------------------------------------------

START_DATE = datetime(year=2026, month=1, day=24, tzinfo=UTC)

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
    # max_active_runs controlled globally via
    # AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
    # Allows inter-DAG parallelism, prevents intra-DAG file conflicts
}
