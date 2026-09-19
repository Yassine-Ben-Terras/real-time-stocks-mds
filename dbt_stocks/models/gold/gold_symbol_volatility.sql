{{
    config(
        materialized='dynamic_table',
        target_lag='5 minutes',
        tags=['gold', 'analytics']
    )
}}

/*
    Per-symbol dispersion over the lookback window, for the treemap.
    Relative volatility (coefficient of variation) is the comparable measure
    across symbols -- raw standard deviation just ranks by share price.

    Target lag is 5 minutes rather than 1: this feeds an overview tile where
    nobody can perceive the difference, and the cheaper refresh cadence is
    free money on the warehouse bill.
*/

select
    symbol,
    count(*)                                                     as candle_count,
    avg(close_price)                                             as avg_price,
    min(low_price)                                               as min_price,
    max(high_price)                                              as max_price,
    sum(volume)                                                  as total_volume,
    sum(notional_value)                                          as total_notional,
    stddev_pop(close_price)                                      as volatility,
    case
        when avg(close_price) = 0 then null
        else stddev_pop(close_price) / nullif(avg(close_price), 0)
    end                                                          as relative_volatility
from {{ ref('gold_ohlc_1m') }}
where candle_ts >= dateadd(day, -{{ var('trade_lookback_days') }}, current_timestamp())
group by symbol
