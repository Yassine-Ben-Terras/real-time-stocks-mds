{{
    config(
        materialized='view',
        tags=['gold', 'serving']
    )
}}

/*
    Latest known state per symbol, plus change against the previous close.

    Deliberately a VIEW, not a dynamic table: it needs `qualify row_number()`
    and a lag over the daily candles, which would force a dynamic table into
    full refresh every minute. As a view it reads a handful of rows off an
    already-materialised aggregate, so it is cheap and always current.
*/

with latest_trade as (

    select
        symbol,
        price          as current_price,
        volume         as last_trade_volume,
        trade_ts       as last_trade_at,
        trade_date
    from {{ ref('silver_stock_trades') }}
    qualify row_number() over (partition by symbol order by trade_ts_ms desc) = 1

),

daily as (

    select
        symbol,
        trade_date,
        min_by(open_price, candle_ts)   as day_open,
        max(high_price)                 as day_high,
        min(low_price)                  as day_low,
        sum(volume)                     as day_volume,
        sum(trade_count)                as day_trade_count
    from {{ ref('gold_ohlc_1m') }}
    group by symbol, trade_date

),

with_prev_close as (

    select
        symbol,
        trade_date,
        day_open,
        day_high,
        day_low,
        day_volume,
        day_trade_count,
        lag(day_close_price) over (partition by symbol order by trade_date) as prev_close
    from (
        select
            d.*,
            c.day_close_price
        from daily d
        left join (
            select
                symbol,
                trade_date,
                max_by(close_price, candle_ts) as day_close_price
            from {{ ref('gold_ohlc_1m') }}
            group by symbol, trade_date
        ) c
            on d.symbol = c.symbol
           and d.trade_date = c.trade_date
    ) as daily_with_close

)

select
    lt.symbol,
    lt.current_price,
    lt.last_trade_volume,
    lt.last_trade_at,
    dc.day_open,
    dc.day_high,
    dc.day_low,
    dc.day_volume,
    dc.day_trade_count,
    dc.prev_close,
    lt.current_price - dc.prev_close                                    as change_amount,
    case
        when dc.prev_close is null or dc.prev_close = 0 then null
        else round((lt.current_price - dc.prev_close) / dc.prev_close * 100, 4)
    end                                                                 as change_percent,
    datediff('second', lt.last_trade_at, current_timestamp())           as staleness_seconds
from latest_trade lt
left join with_prev_close dc
    on lt.symbol = dc.symbol
   and lt.trade_date = dc.trade_date
