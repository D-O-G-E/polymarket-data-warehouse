-- One clean row per market: the LATEST fetch of each, with the ~20 fields
-- the marts need parsed out of the raw JSON payload.
--
-- Two Gamma quirks handled here:
--   1. outcomes / outcomePrices / clobTokenIds are JSON-encoded STRINGS
--      inside the payload ('["Yes", "No"]'), so they need a second parse:
--      JSON_VALUE gets the string, JSON_VALUE_ARRAY parses it to an array.
--   2. closedTime uses a nonstandard '+00' offset ('2024-11-06 15:17:41+00')
--      that TIMESTAMP parsing rejects; appending ':00' fixes it.
-- All parsing is SAFE_*: one malformed 2021 market must not break the build.

with latest as (

    select payload, _ingested_at
    from {{ source('polymarket_raw', 'raw_markets') }}
    qualify row_number() over (
        partition by json_value(payload, '$.id')
        order by _ingested_at desc
    ) = 1

),

parsed as (

    select
        json_value(payload, '$.id')                                    as market_id,
        json_value(payload, '$.conditionId')                           as condition_id,
        json_value(payload, '$.question')                              as question,
        json_value(payload, '$.slug')                                  as slug,

        json_value_array(json_value(payload, '$.clobTokenIds'))[safe_offset(0)]
                                                                       as yes_token_id,
        json_value_array(json_value(payload, '$.clobTokenIds'))[safe_offset(1)]
                                                                       as no_token_id,
        safe_cast(
            json_value_array(json_value(payload, '$.outcomePrices'))[safe_offset(0)]
            as float64)                                                as yes_price,
        safe_cast(
            json_value_array(json_value(payload, '$.outcomePrices'))[safe_offset(1)]
            as float64)                                                as no_price,

        safe_cast(json_value(payload, '$.active') as bool)             as is_active,
        safe_cast(json_value(payload, '$.closed') as bool)             as is_closed,
        json_value(payload, '$.umaResolutionStatus')                   as uma_resolution_status,
        safe_cast(json_value(payload, '$.negRisk') as bool)            as is_neg_risk,

        safe_cast(json_value(payload, '$.volumeNum') as float64)       as volume_usd,
        safe_cast(json_value(payload, '$.liquidityNum') as float64)    as liquidity_usd,

        safe_cast(json_value(payload, '$.startDate') as timestamp)     as start_at,
        safe_cast(json_value(payload, '$.endDate') as timestamp)       as end_at,
        coalesce(
            safe_cast(json_value(payload, '$.closedTime') as timestamp),
            safe_cast(concat(json_value(payload, '$.closedTime'), ':00') as timestamp)
        )                                                              as closed_at,
        safe_cast(json_value(payload, '$.createdAt') as timestamp)     as created_at,
        safe_cast(json_value(payload, '$.updatedAt') as timestamp)     as updated_at,

        _ingested_at                                                   as ingested_at
    from latest

)

select * from parsed
where market_id is not null
