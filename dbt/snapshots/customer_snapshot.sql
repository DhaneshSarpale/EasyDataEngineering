{# ================================================================
   dbt snapshot = SCD Type 2 managed by dbt.
   dbt tracks changes to the tracked columns and maintains
   dbt_valid_from / dbt_valid_to automatically, giving you a versioned
   customer dimension without hand-writing the MERGE.
   ================================================================ #}

{% snapshot customer_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['city', 'country', 'segment'],
      invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    first_name,
    last_name,
    segment,
    city,
    country,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
