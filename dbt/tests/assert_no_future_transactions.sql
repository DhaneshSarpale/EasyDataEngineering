-- Singular (bespoke) dbt test.
-- Passes when it returns zero rows: there must be no transactions dated
-- in the future. Complements the schema tests in _marts.yml.

select transaction_id, transaction_ts
from {{ ref('fct_transactions') }}
where transaction_ts > current_timestamp
