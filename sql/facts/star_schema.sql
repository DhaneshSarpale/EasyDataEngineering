-- ==================================================================
-- DataForge :: Star schema DDL + fact load
-- Dimensions carry surrogate keys; facts reference them.
-- Redshift dialect (DISTKEY/SORTKEY); Athena DDL variant in comments.
-- ==================================================================

-- ---- Dimensions ---------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk   VARCHAR(32) NOT NULL,      -- surrogate key (PK)
    customer_id   VARCHAR(20) NOT NULL,      -- natural key
    first_name    VARCHAR(80),
    last_name     VARCHAR(80),
    segment       VARCHAR(20),
    city          VARCHAR(80),
    country       VARCHAR(4)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dim_account (
    account_sk    VARCHAR(32) NOT NULL,
    account_id    VARCHAR(20) NOT NULL,
    customer_id   VARCHAR(20),
    account_type  VARCHAR(20),
    currency      VARCHAR(4),
    status        VARCHAR(20)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_sk   VARCHAR(32) NOT NULL,
    merchant_id   VARCHAR(20) NOT NULL,
    merchant_name VARCHAR(200),
    category      VARCHAR(40),
    country       VARCHAR(4)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk       INTEGER NOT NULL,          -- yyyymmdd
    date          DATE,
    year          SMALLINT,
    quarter       SMALLINT,
    month         SMALLINT,
    day           SMALLINT,
    day_of_week   VARCHAR(10)
) DISTSTYLE ALL SORTKEY (date_sk);

-- ---- Fact (grain: one row per transaction) ------------------------
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_id   VARCHAR(24) NOT NULL,
    account_sk       VARCHAR(32),            -- FK -> dim_account
    merchant_sk      VARCHAR(32),            -- FK -> dim_merchant
    date_sk          INTEGER,                -- FK -> dim_date
    transaction_type VARCHAR(20),
    amount           DECIMAL(18,2),          -- additive measure
    currency         VARCHAR(4),
    channel          VARCHAR(12),
    status           VARCHAR(12),
    fraud_flag       SMALLINT
)
DISTKEY (account_sk)
SORTKEY (date_sk);

-- ---- Load the fact from silver, resolving surrogate keys ----------
INSERT INTO fact_transaction
SELECT
    t.transaction_id,
    da.account_sk,
    dm.merchant_sk,
    CAST(TO_CHAR(t.transaction_timestamp, 'YYYYMMDD') AS INTEGER) AS date_sk,
    t.transaction_type,
    t.amount,
    t.currency,
    t.channel,
    t.status,
    t.fraud_flag
FROM silver_transactions t
LEFT JOIN dim_account  da ON da.account_id  = t.account_id
LEFT JOIN dim_merchant dm ON dm.merchant_id = t.merchant_id;

-- Athena (external, partitioned) fact variant:
-- CREATE EXTERNAL TABLE fact_transaction (...)
-- PARTITIONED BY (year INT, month INT, day INT)
-- STORED AS PARQUET LOCATION 's3://dataforge-dev-gold/fact_transaction/'
-- TBLPROPERTIES ('parquet.compression'='SNAPPY');
