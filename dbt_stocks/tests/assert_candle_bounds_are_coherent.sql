-- A candle whose high is below its low, or whose open/close falls outside
-- the high/low band, means the aggregation is broken. Should return no rows.
select
    symbol,
    candle_ts,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('gold_ohlc_1m') }}
where high_price < low_price
   or open_price > high_price
   or open_price < low_price
   or close_price > high_price
   or close_price < low_price
