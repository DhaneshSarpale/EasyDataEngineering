with source as (
    select * from {{ source('silver', 'customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    segment,
    city,
    country,
    cast(updated_at as timestamp) as updated_at
from source
