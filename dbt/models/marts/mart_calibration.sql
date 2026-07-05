-- The flagship: is Polymarket calibrated? When the market says 70%, does
-- the event happen ~70% of the time?
--
-- Method: for every resolved market, take its price at FIXED HORIZONS
-- before resolution (24h / 7d / 30d). Fixed horizons matter: a price of
-- 0.99 five minutes before resolution is trivially "calibrated"; the
-- interesting question is how informative prices were with real time
-- left. The as-of join takes the last known price at or before the
-- target moment, within a 50h tolerance so coarse (12h/24h-fidelity)
-- backfilled series still qualify while truly stale prices don't.
--
-- Output grain: one row per (horizon, price bucket), with the observed
-- outcome rate, a 95% Wilson score interval (safer than the normal
-- approximation in thin, near-0/1 buckets), and the Brier score.
-- Perfect calibration: outcome_rate ≈ avg_price in every bucket.

with horizons as (

    select horizon_hours
    from unnest([24, 168, 720]) as horizon_hours  -- 1 day, 1 week, 30 days

),

targets as (

    select
        r.market_id,
        r.yes_token_id,
        r.outcome,
        r.volume_usd,
        h.horizon_hours,
        timestamp_sub(r.resolved_at, interval h.horizon_hours hour) as target_ts
    from {{ ref('fct_resolutions') }} r
    cross join horizons h

),

-- Last price at or before each target moment (the "as-of" join).
priced as (

    select
        t.market_id,
        t.horizon_hours,
        t.outcome,
        t.volume_usd,
        p.price,
        p.fidelity_minutes,
        row_number() over (
            partition by t.market_id, t.horizon_hours
            order by p.price_ts desc
        ) as recency_rank
    from targets t
    join {{ ref('fct_prices') }} p
      on p.token_id = t.yes_token_id
     and p.price_ts <= t.target_ts
     and p.price_ts > timestamp_sub(t.target_ts, interval 50 hour)

),

scored as (

    select
        horizon_hours,
        least(cast(floor(price * 10) as int64), 9) as price_bucket,  -- 0..9 deciles
        price,
        if(outcome = 'yes', 1, 0)                  as outcome_int
    from priced
    where recency_rank = 1

),

aggregated as (

    select
        horizon_hours,
        price_bucket,
        count(*)                                   as n_markets,
        avg(price)                                 as avg_price,
        avg(outcome_int)                           as outcome_rate,
        avg(pow(price - outcome_int, 2))           as brier_score
    from scored
    group by 1, 2

)

-- 95% Wilson score interval on the outcome rate.
select
    horizon_hours,
    price_bucket,
    n_markets,
    avg_price,
    outcome_rate,
    (outcome_rate + pow(1.96, 2) / (2 * n_markets)
        - 1.96 * sqrt((outcome_rate * (1 - outcome_rate)
                       + pow(1.96, 2) / (4 * n_markets)) / n_markets))
        / (1 + pow(1.96, 2) / n_markets)           as wilson_low,
    (outcome_rate + pow(1.96, 2) / (2 * n_markets)
        + 1.96 * sqrt((outcome_rate * (1 - outcome_rate)
                       + pow(1.96, 2) / (4 * n_markets)) / n_markets))
        / (1 + pow(1.96, 2) / n_markets)           as wilson_high,
    brier_score
from aggregated
order by horizon_hours, price_bucket
