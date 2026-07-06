-- The market dimension: one row per market, current state.
-- (Historical states live in the markets_snapshot SCD2 table.)

select
    market_id,
    condition_id,
    question,
    slug,
    event_id,
    event_slug,
    yes_token_id,
    no_token_id,
    is_active,
    is_closed,
    is_neg_risk,
    uma_resolution_status,
    volume_usd,
    liquidity_usd,
    start_at,
    end_at,
    closed_at,
    created_at,
    updated_at
from {{ ref('stg_markets') }}
