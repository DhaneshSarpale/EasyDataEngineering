-- ==================================================================
-- DataForge :: Amazon Redshift warehouse setup
-- Staging + dimension + fact DDL, COPY from S3, UPSERT via staging,
-- and maintenance (VACUUM / ANALYZE). Demonstrates distribution styles,
-- sort keys, and column compression (encoding).
-- ==================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ---- Staging table (loaded from S3 silver zone) -------------------
-- Staging is transient; DISTSTYLE EVEN + no compression is fine here.
CREATE TABLE IF NOT EXISTS staging.stg_transactions (
    transaction_id   VARCHAR(24),
    account_id       VARCHAR(20),
    merchant_id      VARCHAR(20),
    transaction_type VARCHAR(20),
    amount           DECIMAL(18,2),
    currency         VARCHAR(4),
    channel          VARCHAR(12),
    status           VARCHAR(12),
    fraud_flag       SMALLINT,
    transaction_timestamp TIMESTAMP
) DISTSTYLE EVEN;

-- COPY bulk-loads Parquet from S3 in parallel across slices. Auth via an
-- attached IAM role - never inline keys.
COPY staging.stg_transactions
FROM 's3://dataforge-dev-silver/transactions/'
IAM_ROLE 'arn:aws:iam::000000000000:role/dataforge-dev-redshift-copy'
FORMAT AS PARQUET;

-- ---- Fact table with performance-oriented physical design ---------
-- DISTKEY on the most-joined key co-locates matching rows on the same
-- slice (avoids a redistribution during joins). SORTKEY on the date
-- enables zone-map pruning for time-range filters. ENCODE picks a
-- compression codec per column (AZ64/ZSTD) to cut I/O.
CREATE TABLE IF NOT EXISTS analytics.fact_transaction (
    transaction_id   VARCHAR(24) NOT NULL ENCODE ZSTD,
    account_sk       VARCHAR(32)          ENCODE ZSTD,
    merchant_sk      VARCHAR(32)          ENCODE ZSTD,
    date_sk          INTEGER              ENCODE AZ64,
    transaction_type VARCHAR(20)          ENCODE BYTEDICT,
    amount           DECIMAL(18,2)        ENCODE AZ64,
    currency         VARCHAR(4)           ENCODE BYTEDICT,
    channel          VARCHAR(12)          ENCODE BYTEDICT,
    status           VARCHAR(12)          ENCODE BYTEDICT,
    fraud_flag       SMALLINT             ENCODE AZ64
)
DISTKEY (account_sk)
SORTKEY (date_sk);

-- ---- UPSERT (Redshift best-practice: staging + delete + insert) ----
BEGIN;
    -- Remove rows that are being replaced by the new batch.
    DELETE FROM analytics.fact_transaction
    USING staging.stg_transactions s
    WHERE analytics.fact_transaction.transaction_id = s.transaction_id;

    -- Insert the new/updated rows (surrogate keys resolved by a prior step).
    INSERT INTO analytics.fact_transaction
    SELECT
        s.transaction_id,
        'ACCSK_' || SUBSTRING(MD5(s.account_id) FROM 1 FOR 12),
        'MERSK_' || SUBSTRING(MD5(s.merchant_id) FROM 1 FOR 12),
        CAST(TO_CHAR(s.transaction_timestamp, 'YYYYMMDD') AS INTEGER),
        s.transaction_type, s.amount, s.currency, s.channel, s.status, s.fraud_flag
    FROM staging.stg_transactions s;
COMMIT;

-- ---- Maintenance --------------------------------------------------
-- VACUUM reclaims space and re-sorts rows after big DELETE/UPDATE churn.
VACUUM DELETE ONLY analytics.fact_transaction;
VACUUM SORT ONLY  analytics.fact_transaction;
-- ANALYZE refreshes table statistics so the planner picks good plans.
ANALYZE analytics.fact_transaction;

-- Redshift Spectrum: query the S3 gold zone without loading it -------
-- CREATE EXTERNAL SCHEMA spectrum FROM DATA CATALOG
--   DATABASE 'dataforge_dev'
--   IAM_ROLE 'arn:aws:iam::000000000000:role/dataforge-dev-redshift-spectrum';
-- SELECT * FROM spectrum.gold_daily_summary LIMIT 10;
