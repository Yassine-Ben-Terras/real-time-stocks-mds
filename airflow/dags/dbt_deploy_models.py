"""Reconcile the warehouse with the repository.

Dynamic tables refresh their *data* on their own, but their *definition* only
changes when dbt runs. This DAG is the thing that applies a merged SQL change
to production, and it is also triggered manually by the CD workflow.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

from dbt_helpers import DEFAULT_ARGS, dbt_command

with DAG(
    dag_id="dbt_deploy_models",
    description="Apply model definitions and regenerate docs",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "deploy"],
) as dag:

    deps = BashOperator(task_id="dbt_deps", bash_command=dbt_command("deps"))

    build = BashOperator(
        task_id="dbt_build",
        bash_command=dbt_command("build", extra="--fail-fast"),
    )

    docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=dbt_command("docs generate"),
    )

    deps >> build >> docs
