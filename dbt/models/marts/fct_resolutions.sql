-- One row per RESOLVED binary market with an unambiguous outcome.
--
-- Outcome derivation (documented because it's an inference, not a field):
-- a resolved market's outcomePrices go terminal — ["1","0"] means Yes
-- won, ["0","1"] means No. We require is_closed AND terminal prices; the
-- oracle status (umaResolutionStatus = 'resolved') is carried as a
-- cross-check flag rather than a hard filter, since old markets predate
-- the field. Markets with non-terminal final prices (e.g. 50/50 refunds,
-- ties) are deliberately excluded — they have no binary outcome to score.

with resolved as (

    select
        market_id,
        condition_id,
        question,
        slug,
        yes_token_id,
        volume_usd,
        end_at,
        closed_at,
        case
            when yes_price = 1 and no_price = 0 then 'yes'
            when yes_price = 0 and no_price = 1 then 'no'
        end                                                     as outcome,
        (uma_resolution_status = 'resolved')                    as oracle_confirms,
        -- Best available resolution moment: explicit close time when
        -- present, else the scheduled end date.
        coalesce(closed_at, end_at)                             as resolved_at
    from {{ ref('stg_markets') }}
    where is_closed

)

select *
from resolved
where outcome is not null
  and yes_token_id is not null
  and resolved_at is not null
