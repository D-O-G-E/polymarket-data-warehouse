-- Question 4 of the catalog: within a multi-outcome event ("Who wins the
-- World Cup?"), the Yes prices of its markets should sum to ~1 — exactly
-- one outcome will happen. Persistent deviation is either an arbitrage
-- gap or a market-mechanics artifact; either way it's measurable here.
--
-- Grain: one row per (event, day). Scope: negRisk events only (their
-- markets are mutually exclusive by construction; ordinary events can
-- hold unrelated questions where no sum rule applies), with 3+ markets,
-- and only days where EVERY market in the event has a price — a partial
-- sum says nothing about coherence.
--
-- Known caveat: markets are compared against the event's CURRENT roster;
-- for dates before a late-added market existed, full coverage simply
-- won't occur and the day is excluded rather than misstated.

with event_markets as (

    select event_id, market_id, yes_token_id
    from {{ ref('dim_markets') }}
    where is_neg_risk
      and event_id is not null
      and yes_token_id is not null

),

multi_outcome as (

    select event_id, count(*) as n_markets
    from event_markets
    group by event_id
    having count(*) >= 3

),

-- End-of-day price per token.
daily_price as (

    select
        token_id,
        price_date,
        array_agg(price order by price_ts desc limit 1)[offset(0)] as eod_price
    from {{ ref('fct_prices') }}
    group by token_id, price_date

),

summed as (

    select
        em.event_id,
        mo.n_markets,
        dp.price_date,
        count(*)          as n_priced,
        sum(dp.eod_price) as sum_yes_prices
    from event_markets em
    join multi_outcome mo using (event_id)
    join daily_price dp on dp.token_id = em.yes_token_id
    group by em.event_id, mo.n_markets, dp.price_date

)

select
    event_id,
    price_date,
    n_markets,
    sum_yes_prices,
    sum_yes_prices - 1 as deviation
from summed
where n_priced = n_markets
