-- Mart: customer monthly spend (a Customer-360 building block).
-- Materialised as a table so BI tools query it directly.

with fct as (
    select * from {{ ref('fct_transactions') }}
)

select
    customer_id,
    segment_month.year,
    segment_month.month,
    count(*)                                    as txn_count,
    sum(amount)                                 as total_spend,
    avg(amount)                                 as avg_txn_value,
    sum(fraud_flag)                             as fraud_txns
from fct
cross join lateral (
    select
        extract(year  from transaction_ts) as year,
        extract(month from transaction_ts) as month
) as segment_month
group by customer_id, segment_month.year, segment_month.month
