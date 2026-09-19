{{
    config(
        materialized='dynamic_table',
        target_lag='1 minute',
        tags=['gold', 'streaming']
    )
}}

/*
    True OHLC candles at one-minute grain, built from actual trades.

    This is the model the previous version of the project got wrong: it
    derived "open" from the first *polled* snapshot of the day, which is not
    the market open. With a real trade stream, open and close are simply the
    first and last trade in the bucket -- expressed here with MIN_BY/MAX_BY
    so the whole model stays a pure aggregate.
*/

select
    symbol,
    trade_minute                                     as candle_ts,
    cast(trade_minute as date)                       as trade_date,

    min_by(price, trade_ts_ms)                       as open_price,
    max(price)                                       as high_price,
    min(price)                                       as low_price,
    max_by(price, trade_ts_ms)                       as close_price,

    sum(volume)                                      as volume,
    sum(notional_value)                              as notional_value,
    count(*)                                         as trade_count,
    avg(price)                                       as vwap_approx,
    stddev_pop(price)                                as intra_minute_volatility,
    max(ingest_latency_ms)                           as max_ingest_latency_ms

from {{ ref('silver_stock_trades') }}
group by symbol, trade_minute
