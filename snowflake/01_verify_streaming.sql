/* Diagnostics for when trades are not showing up. Run in order. */

use role stocks_transformer;
use database stocks_mds;

-- 1. Is the connector writing at all?
select count(*) as raw_rows,
       max(to_timestamp_ntz(record_metadata:CreateTime::number, 3)) as last_write_at
from raw.stock_trades_raw;

-- 2. What does one record look like?
select record_content, record_metadata
from raw.stock_trades_raw
order by record_metadata:CreateTime::number desc
limit 5;

-- 3. End-to-end latency, exchange timestamp to warehouse arrival.
select
    record_content:symbol::varchar as symbol,
    count(*) as trades,
    avg(datediff('millisecond',
        try_to_timestamp_ntz(record_content:trade_ts::varchar),
        to_timestamp_ntz(record_metadata:CreateTime::number, 3))) as avg_latency_ms
from raw.stock_trades_raw
where to_timestamp_ntz(record_metadata:CreateTime::number, 3)
      >= dateadd(hour, -1, current_timestamp())
group by 1
order by 1;

-- 4. Are the dynamic tables refreshing, and how much are they costing?
select name, state, refresh_start_time, refresh_end_time,
       statistics:numInsertedRows::number as inserted_rows
from table(information_schema.dynamic_table_refresh_history())
order by refresh_start_time desc
limit 20;

-- 5. Anything stuck?
select name, scheduling_state
from table(information_schema.dynamic_tables())
where scheduling_state:reason_code is not null;
