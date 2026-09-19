-- Trades timestamped in the future indicate a unit mix-up (seconds parsed as
-- milliseconds, or the reverse). A 5 minute allowance covers clock skew.
select
    symbol,
    trade_ts,
    trade_ts_ms
from {{ ref('silver_stock_trades') }}
where trade_ts > dateadd(minute, 5, current_timestamp())
