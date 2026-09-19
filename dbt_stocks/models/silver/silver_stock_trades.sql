{{
    config(
        materialized='dynamic_table',
        target_lag='1 minute',
        tags=['silver', 'streaming']
    )
}}

/*
    Deduplicated, validated trade stream.

    Snowpipe Streaming is at-least-once, and the producer may replay after a
    restart, so the same trade can land more than once. We dedupe on the
    natural key (symbol, exchange timestamp, price, volume).

    Implementation note: this uses GROUP BY rather than
    `qualify row_number() over (...)`. Both express the same intent, but an
    aggregate keeps the dynamic table eligible for INCREMENTAL refresh --
    a window function would force Snowflake into a full refresh on every
    cycle, which is exactly the cost profile we are trying to avoid.
*/

with validated as (

    select *
    from {{ ref('bronze_stock_trades') }}
    where price > 0
      and volume >= 0
      and trade_ts is not null
      and symbol is not null
      -- guard against clock-skewed or replayed ancient records
      and trade_ts >= dateadd(day, -{{ var('trade_lookback_days') }}, current_timestamp())
      and trade_ts <= dateadd(minute, 5, current_timestamp())

),

deduplicated as (

    select
        symbol,
        trade_ts_ms,
        price,
        volume,

        min(trade_ts)          as trade_ts,
        min(ingested_at)       as ingested_at,
        min(kafka_offset)      as kafka_offset,
        min(kafka_partition)   as kafka_partition,
        count(*)               as duplicate_count

    from validated
    group by symbol, trade_ts_ms, price, volume

)

select
    symbol,
    price,
    volume,
    price * volume                                  as notional_value,
    trade_ts_ms,
    trade_ts,
    date_trunc('minute', trade_ts)                  as trade_minute,
    cast(trade_ts as date)                          as trade_date,
    ingested_at,
    -- observability: how long the record took to get from exchange to warehouse
    datediff('millisecond', trade_ts, ingested_at)  as ingest_latency_ms,
    kafka_partition,
    kafka_offset,
    duplicate_count
from deduplicated
