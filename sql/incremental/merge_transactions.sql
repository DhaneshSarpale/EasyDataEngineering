-- ==================================================================
-- DataForge :: Incremental warehouse load via MERGE (UPSERT)
-- Loads only new/changed rows staged from the incremental extract
-- (WHERE updated_at > watermark) into the target fact table.
-- Redshift/Snowflake MERGE syntax.
-- ==================================================================

-- Staging table holds just this batch's rows (loaded via COPY from S3).
-- CREATE TEMP TABLE stg_transactions (LIKE fact_transaction_src);
-- COPY stg_transactions FROM 's3://dataforge-dev-silver/transactions_incr/'
--   IAM_ROLE 'arn:aws:iam::<acct>:role/dataforge-redshift-copy'
--   FORMAT AS PARQUET;

MERGE INTO fact_transaction AS tgt
USING stg_transactions AS src
    ON tgt.transaction_id = src.transaction_id
WHEN MATCHED THEN UPDATE SET
    amount     = src.amount,
    status     = src.status,
    fraud_flag = src.fraud_flag
WHEN NOT MATCHED THEN INSERT
    (transaction_id, account_sk, merchant_sk, date_sk,
     transaction_type, amount, currency, channel, status, fraud_flag)
    VALUES
    (src.transaction_id, src.account_sk, src.merchant_sk, src.date_sk,
     src.transaction_type, src.amount, src.currency, src.channel,
     src.status, src.fraud_flag);

-- Portable alternative for engines without MERGE (delete + insert):
-- BEGIN;
--   DELETE FROM fact_transaction
--   USING stg_transactions s
--   WHERE fact_transaction.transaction_id = s.transaction_id;
--   INSERT INTO fact_transaction SELECT * FROM stg_transactions;
-- COMMIT;

-- The watermark in the metadata store is advanced ONLY after this
-- transaction commits successfully (see MetadataStore.commit_watermark).
