"""Shared Airflow defaults for all DAG factories.

Centralizes production settings
(start date, task retries/timeouts, DAG-level concurrency) so that
``to_silver_factory``, ``to_silver_pg_factory`` and ``to_gold_factory`` stay in sync.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

# --------------------------------------------------------------------------------------
# DAG start date
# --------------------------------------------------------------------------------------


START_DATE = datetime(year=2026, month=4, day=23, tzinfo=UTC)


# --------------------------------------------------------------------------------------
# Default task arguments
# --------------------------------------------------------------------------------------

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
}


# --------------------------------------------------------------------------------------
# Shared DAG-level kwargs
# --------------------------------------------------------------------------------------


# Spread into every factory's @dag() call so the three factories stay in sync.
# Per-DAG identity (dag_id, schedule, tags, description) and doc_md=__doc__ stay at
# each call site.
#
# max_active_runs=1: one active run per DAG serializes intra-DAG runs (scheduled +
# manual, and catchup once enabled per TODO[prod]) so they never race on the shared
# data/ files, while allowing inter-DAG parallelism. Set here, not via
# AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG, so the invariant travels with the code and
# holds in any deployment. CLI ↔ Airflow races are out of scope (see README).
DAG_COMMON_KWARGS: dict[str, Any] = {
    "start_date": START_DATE,
    "catchup": False,  # TODO[prod]: set to True
    "default_args": DEFAULT_ARGS,
    "max_active_runs": 1,
}
