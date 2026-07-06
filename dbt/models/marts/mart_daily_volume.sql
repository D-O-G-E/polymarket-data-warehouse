-- Question 5 of the catalog: volume structure over time. The API only
-- exposes LIFETIME cumulative volume per market — no daily series exists
-- anywhere upstream. But the SCD2 snapshot records every observed value
-- of that cumulative number, so differencing successive end-of-day
-- versions DERIVES the daily series: a fact conjured from a dimension's
-- history. This mart therefore only gets richer with snapshot age
-- (history begins 2026-07-05); early rows may span multiple days —
-- days_since_prev says exactly how many, so consumers can normalize.
--
-- Grain: one row per (market_id, snapshot date with a change).

with eod_versions as (

    select
        market_id,
        date(dbt_valid_from) as snap_date,
        volume_usd,
        row_number() over (
            partition by market_id, date(dbt_valid_from)
            order by dbt_valid_from desc
        ) as rn
    from {{ ref('markets_snapshot') }}
    where volume_usd is not null

),

eod as (

    select market_id, snap_date, volume_usd
    from eod_versions
    where rn = 1

),

diffed as (

    select
        market_id,
        snap_date,
        volume_usd as cum_volume_usd,
        volume_usd - lag(volume_usd) over (
            partition by market_id order by snap_date
        ) as volume_delta_usd,
        date_diff(
            snap_date,
            lag(snap_date) over (partition by market_id order by snap_date),
            day
        ) as days_since_prev
    from eod

)

select *
from diffed
where volume_delta_usd is not null
