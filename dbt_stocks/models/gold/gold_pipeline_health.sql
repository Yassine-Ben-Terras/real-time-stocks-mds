{{
    config(
        materialized='view',
        tags=['gold', 'observability']
    )
}}

/*
    Self-monitoring model. Put this on a page of the dashboard: it shows the
    pipeline watching itself, which is the part most portfolio projects skip.
*/

select
    symbol,
    count(*)                                                    as trades_last_hour,
    max(trade_ts)                                               as last_trade_at,
    datediff('second', max(trade_ts), current_timestamp())      as seconds_since_last_trade,
    avg(ingest_latency_ms)                                      as avg_ingest_latency_ms,
    percentile_cont(0.95) within group (order by ingest_latency_ms)
                                                                as p95_ingest_latency_ms,
    max(ingest_latency_ms)                                      as max_ingest_latency_ms,
    sum(case when duplicate_count > 1 then 1 else 0 end)        as deduplicated_trades
from {{ ref('silver_stock_trades') }}
where trade_ts >= dateadd(hour, -1, current_timestamp())
group by symbol
