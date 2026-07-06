-- Question 3 of the catalog: how early does the market "decide", and how
-- does day-to-day movement behave as resolution approaches?
--
-- Grain: one row per days_before_resolution (0–30), aggregated across
-- all resolved markets. Two curves come out of it:
--   avg_abs_error      — mean |price − outcome|: how far, on average,
--                        the market still is from the eventual answer
--                        with N days left ("decisiveness").
--   avg_abs_daily_move — mean |EOD price − prior EOD price|: how much
--                        prices move per day at that distance
--                        ("late-breaking information").

with daily as (

    select
        r.market_id,
        if(r.outcome = 'yes', 1, 0) as outcome_int,
        date_diff(date(r.resolved_at), p.price_date, day) as days_before,
        array_agg(p.price order by p.price_ts desc limit 1)[offset(0)] as eod_price
    from {{ ref('fct_resolutions') }} r
    join {{ ref('fct_prices') }} p
      on p.token_id = r.yes_token_id
     and p.price_ts <= r.resolved_at
    where date_diff(date(r.resolved_at), p.price_date, day) between 0 and 30
    group by r.market_id, outcome_int, days_before

),

with_moves as (

    select
        *,
        abs(eod_price - lag(eod_price) over (
            partition by market_id order by days_before desc
        )) as abs_daily_move
    from daily

)

select
    days_before,
    count(distinct market_id)          as n_markets,
    avg(abs(eod_price - outcome_int))  as avg_abs_error,
    avg(abs_daily_move)                as avg_abs_daily_move
from with_moves
group by days_before
order by days_before
