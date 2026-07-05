{% snapshot markets_snapshot %}

{#
  SCD2 history of market metadata. Market rows CHANGE over time —
  questions get edited, end dates move, volume accumulates — and
  sync-catalog re-lands the current state every run. This snapshot turns
  those re-lands into validity-interval history (dbt_valid_from /
  dbt_valid_to), keyed on Gamma's own updatedAt timestamp.

  Enables, among others: daily volume derived by differencing successive
  versions of cumulative volume_usd — a fact series conjured from a
  dimension's history.
#}

{{
    config(
        target_schema='polymarket_snapshots',
        unique_key='market_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select
    market_id,
    question,
    slug,
    is_active,
    is_closed,
    uma_resolution_status,
    volume_usd,
    liquidity_usd,
    end_at,
    updated_at
from {{ ref('stg_markets') }}
where updated_at is not null

{% endsnapshot %}
