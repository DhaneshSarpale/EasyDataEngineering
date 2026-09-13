with source as (
    select * from {{ source('silver', 'accounts') }}
)

select
    account_id,
    customer_id,
    account_type,
    currency,
    status
from source
