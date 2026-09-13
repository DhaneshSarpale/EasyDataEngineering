-- Staging: light cleaning & renaming of the silver transactions source.
-- Materialised as a view (cheap, always fresh). No business logic here -
-- staging just conforms types and standardises names.

with source as (
    select * from {{ source('silver', 'transactions') }}
)

select
    transaction_id,
    account_id,
    merchant_id,
    transaction_type,
    cast(amount as {{ dbt.type_numeric() }})            as amount,
    upper(currency)                                     as currency,
    channel,
    status,
    cast(fraud_flag as integer)                         as fraud_flag,
    cast(transaction_timestamp as timestamp)            as transaction_ts
from source
-- drop rows that should never reach analytics (defensive; silver is clean)
where account_id is not null
  and amount >= 0
