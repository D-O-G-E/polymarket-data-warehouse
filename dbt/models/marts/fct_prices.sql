-- The price fact table: one row per (token, timestamp), date-partitioned
-- and clustered for cheap "one token's history" scans.
--
-- Incremental: each run merges only rows ingested since the last build
-- (keyed on price_id, so re-processing can never duplicate). A full
-- rebuild is always available with `dbt build --full-refresh`.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='price_id',
    partition_by={'field': 'price_date', 'data_type': 'date'},
    cluster_by=['token_id'],
) }}

select
    price_id,
    token_id,
    market_id,
    condition_id,
    price_ts,
    price_date,
    price,
    fidelity_minutes,
    ingested_at
from {{ ref('stg_prices') }}

{% if is_incremental() %}
where ingested_at > (
    select coalesce(max(ingested_at), timestamp('1970-01-01'))
    from {{ this }}
)
{% endif %}
