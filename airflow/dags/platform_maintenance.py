"""Daily housekeeping: cost visibility and archive retention.

Dynamic tables are convenient and can quietly get expensive. This DAG writes
a daily credit-consumption snapshot into an ops table so the cost of the
architecture is visible rather than discovered on an invoice.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator

from dbt_helpers import DEFAULT_ARGS

SNOWFLAKE_CONN_ID = "snowflake_default"

CAPTURE_DT_REFRESH_COST = """
create table if not exists ops.dynamic_table_refresh_history (
    captured_at        timestamp_ntz,
    qualified_name     varchar,
    state              varchar,
    refresh_start_time timestamp_ltz,
    refresh_end_time   timestamp_ltz,
    statistics         variant
);

insert into ops.dynamic_table_refresh_history
select
    current_timestamp(),
    name,
    state,
    refresh_start_time,
    refresh_end_time,
    statistics
from table(information_schema.dynamic_table_refresh_history(
    result_limit => 1000
))
where refresh_start_time >= dateadd(day, -1, current_timestamp());
"""

CAPTURE_WAREHOUSE_SPEND = """
create table if not exists ops.warehouse_credit_daily (
    captured_at     timestamp_ntz,
    usage_date      date,
    warehouse_name  varchar,
    credits_used    float
);

insert into ops.warehouse_credit_daily
select
    current_timestamp(),
    cast(start_time as date),
    warehouse_name,
    sum(credits_used)
from snowflake.account_usage.warehouse_metering_history
where start_time >= dateadd(day, -2, current_timestamp())
group by 2, 3;
"""

with DAG(
    dag_id="platform_maintenance",
    description="Cost and refresh telemetry for the streaming warehouse",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="30 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ops", "finops"],
) as dag:

    start = EmptyOperator(task_id="start")

    refresh_cost = SnowflakeSqlApiOperator(
        task_id="capture_dynamic_table_refresh_history",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CAPTURE_DT_REFRESH_COST,
    )

    warehouse_spend = SnowflakeSqlApiOperator(
        task_id="capture_warehouse_credit_usage",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=CAPTURE_WAREHOUSE_SPEND,
    )

    start >> [refresh_cost, warehouse_spend]
