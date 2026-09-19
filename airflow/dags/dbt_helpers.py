"""Shared helpers for the dbt DAGs.

Airflow's role in this architecture is deliberately narrow. Ingestion is
handled by the Snowflake Kafka Connector and transformation refresh is handled
by Snowflake dynamic tables, both of which run continuously without an
orchestrator. What Airflow owns is everything that *is* a scheduled batch
concern: data quality, source freshness, deployment, docs and cost control.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_stocks")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}


def dbt_command(subcommand: str, *, select: str | None = None, extra: str = "") -> str:
    """Build a dbt invocation string for a BashOperator.

    Kept as one function so every DAG passes the same project dir, profiles
    dir and target, and so the command is trivially unit-testable.
    """
    parts = [
        "cd", DBT_PROJECT_DIR, "&&",
        "dbt", subcommand,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--target", DBT_TARGET,
    ]
    if select:
        parts += ["--select", select]
    if extra:
        parts.append(extra)
    return " ".join(parts)
