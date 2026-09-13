-- Intermediate (ephemeral): join transactions to their owning customer via
-- account. Ephemeral means it's inlined as a CTE into downstream marts -
-- no table is materialised, keeping the warehouse tidy.

with txn as (
    select * from {{ ref('stg_transactions') }}
),
acc as (
    select * from {{ ref('stg_accounts') }}
),
cust as (
    select * from {{ ref('stg_customers') }}
)

select
    txn.transaction_id,
    txn.transaction_ts,
    txn.amount,
    txn.currency,
    txn.channel,
    txn.status,
    txn.fraud_flag,
    acc.account_id,
    acc.account_type,
    cust.customer_id,
    cust.segment,
    cust.country
from txn
join acc  on acc.account_id  = txn.account_id
join cust on cust.customer_id = acc.customer_id
