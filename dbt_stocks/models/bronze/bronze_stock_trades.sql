{{
    config(
        materialized='view',
        tags=['bronze', 'streaming']
    )
}}

/*
    Typed, one-to-one projection of the Kafka landing table.

    Design note: the wire format carries `trade_ts_ms` as explicit epoch
    milliseconds AND a pre-rendered ISO-8601 `trade_ts`. We parse the ISO
    string and keep the raw milliseconds alongside it, so no model downstream
    ever has to guess the unit. The Kafka coordinates are kept because they
    are the only true idempotency key we have.
*/

with source as (

    select
        record_content    as payload,
        record_metadata   as meta
    from {{ source('raw', 'stock_trades_raw') }}

),

typed as (

    select
        -- business payload
        payload:symbol::varchar(16)                          as symbol,
        payload:price::float                                 as price,
        payload:volume::float                                as volume,
        payload:trade_ts_ms::number(38, 0)                   as trade_ts_ms,
        try_to_timestamp_ntz(payload:trade_ts::varchar)      as trade_ts,
        try_to_timestamp_ntz(payload:ingested_at::varchar)   as ingested_at,
        payload:conditions                                   as trade_conditions,
        payload:schema_version::varchar(16)                  as schema_version,
        payload:source::varchar(64)                          as source_system,

        -- kafka coordinates: (topic, partition, offset) is globally unique
        meta:topic::varchar(128)                             as kafka_topic,
        meta:partition::number(10, 0)                        as kafka_partition,
        meta:offset::number(38, 0)                           as kafka_offset,
        to_timestamp_ntz(meta:CreateTime::number, 3)         as kafka_created_at

    from source

)

select * from typed
