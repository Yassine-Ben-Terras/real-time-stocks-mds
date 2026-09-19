"""Continuous data quality: run dbt tests and source freshness on a cadence.

Why 15 minutes and not 1: the dynamic tables refresh themselves, so this DAG
is not on the critical path for data arriving. It is a monitor. Running it
every minute would burn warehouse credits to tell you the same thing.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from dbt_helpers import DEFAULT_ARGS, dbt_command

with DAG(
    dag_id="dbt_quality_checks",
    description="dbt source freshness + tests against the live streaming models",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "quality", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
    )

    # Fails loudly if the Kafka connector has stopped writing to RAW.
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=dbt_command("source freshness"),
    )

    test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=dbt_command("test", select="silver"),
    )

    test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=dbt_command("test", select="gold"),
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> deps >> source_freshness >> [test_silver, test_gold] >> finish
