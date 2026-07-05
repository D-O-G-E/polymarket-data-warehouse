-- One row per (token, timestamp): the raw layer's duplicates collapse
-- here. Ingestion is append-only and deliberately refetches overlaps
-- (duplicates are free, gaps are forever), so this dedup is a designed-in
-- step, not a repair.
--
-- Tie-break: prefer the FINEST fidelity (a 60-minute harvest point beats
-- a 720-minute backfill point at the same timestamp), then the most
-- recent ingestion.

select
    concat(token_id, ':', cast(t as string)) as price_id,
    token_id,
    market_id,
    condition_id,
    timestamp_seconds(t)                     as price_ts,
    date(timestamp_seconds(t))               as price_date,
    p                                        as price,
    fidelity_minutes,
    _ingested_at                             as ingested_at
from {{ source('polymarket_raw', 'raw_price_history') }}
where token_id is not null
  and t is not null
  and p is not null
qualify row_number() over (
    partition by token_id, t
    order by fidelity_minutes asc, _ingested_at desc
) = 1
