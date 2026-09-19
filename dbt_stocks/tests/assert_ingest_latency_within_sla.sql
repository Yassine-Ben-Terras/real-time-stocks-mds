-- The whole point of this architecture is sub-minute latency. If the p95 for
-- any symbol in the last hour exceeds 60s, the streaming path has degraded.
select
    symbol,
    p95_ingest_latency_ms
from {{ ref('gold_pipeline_health') }}
where p95_ingest_latency_ms > 60000
