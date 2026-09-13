-- ==================================================================
-- DataForge :: Athena setup
-- External tables over the S3 lake, partition management, CTAS, and views.
-- Athena is serverless & bills per TB scanned - partitioning + columnar
-- Parquet + column pruning are what keep queries cheap.
-- ==================================================================

-- Database (points at the Glue Data Catalog) ------------------------
CREATE DATABASE IF NOT EXISTS dataforge_dev;

-- External table over the SILVER transactions zone (partitioned) -----
CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_dev.silver_transactions (
    transaction_id   string,
    account_id       string,
    merchant_id      string,
    transaction_type string,
    amount           decimal(18,2),
    currency         string,
    channel          string,
    status           string,
    fraud_flag       int,
    transaction_timestamp timestamp
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://dataforge-dev-silver/transactions/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Register partitions (either MSCK, or partition projection). ---------
-- Option A: discover partitions from S3 layout:
MSCK REPAIR TABLE dataforge_dev.silver_transactions;

-- Option B (preferred, no crawl needed): partition projection.
-- ALTER TABLE dataforge_dev.silver_transactions SET TBLPROPERTIES (
--   'projection.enabled'='true',
--   'projection.year.type'='integer',  'projection.year.range'='2024,2026',
--   'projection.month.type'='integer', 'projection.month.range'='1,12',
--   'storage.location.template'='s3://dataforge-dev-silver/transactions/year=${year}/month=${month}/'
-- );

-- CTAS: build a gold aggregate table directly from a query -----------
-- CTAS writes results back to S3 as Parquet and registers a table -
-- a common way to materialise a mart in a lakehouse.
CREATE TABLE dataforge_dev.gold_daily_summary
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://dataforge-dev-gold/daily_transaction_summary/',
    partitioned_by = ARRAY['currency']
) AS
SELECT
    CAST(transaction_timestamp AS date) AS transaction_date,
    COUNT(*)                            AS txn_count,
    COUNT(DISTINCT account_id)          AS distinct_accounts,
    SUM(amount)                         AS total_amount,
    ROUND(AVG(amount), 2)               AS avg_amount,
    SUM(fraud_flag)                     AS fraud_count,
    currency
FROM dataforge_dev.silver_transactions
GROUP BY CAST(transaction_timestamp AS date), currency;

-- A view for BI tools (no storage cost; query cost only on use) ------
CREATE OR REPLACE VIEW dataforge_dev.v_high_value_txns AS
SELECT transaction_id, account_id, amount, currency, channel, transaction_timestamp
FROM dataforge_dev.silver_transactions
WHERE amount >= 5000;

-- Complex JSON query example (Athena can query nested JSON directly) --
-- Assuming a raw_events table with a struct 'payload':
-- SELECT event_id, payload.amount, device.country
-- FROM dataforge_dev.raw_events
-- WHERE payload.currency = 'EUR';
