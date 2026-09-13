-- Mart: incremental transaction fact.
-- Incremental models only process new rows on subsequent runs (matched by
-- the `is_incremental()` filter), which is how you keep large facts cheap
-- to refresh. On first run it builds the whole table.

{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
  )
}}

with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    transaction_id,
    customer_id,
    account_id,
    transaction_ts,
    cast(transaction_ts as date)                        as transaction_date,
    amount,
    currency,
    channel,
    status,
    fraud_flag
from enriched

{% if is_incremental() %}
-- Only pull rows newer than what we've already loaded (watermark).
where transaction_ts > (select coalesce(max(transaction_ts), '1900-01-01') from {{ this }})
{% endif %}
