-- ==================================================================
-- DataForge :: Window function catalogue
-- Portable ANSI SQL (runs on Athena/Trino, Redshift, and SQLite 3.25+).
-- Each query mirrors a function in
--   src/dataforge/transformations/window_functions.py
-- ==================================================================

-- ROW_NUMBER: latest version of each customer -----------------------
SELECT *
FROM (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY updated_at DESC
           ) AS rn
    FROM silver_customers c
) ranked
WHERE rn = 1;

-- RANK / DENSE_RANK: rank transactions by amount within an account ---
SELECT transaction_id,
       account_id,
       amount,
       RANK()       OVER (PARTITION BY account_id ORDER BY amount DESC) AS amount_rank,
       DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS amount_dense_rank
FROM silver_transactions;

-- LAG / LEAD: previous & next transaction amount per account ---------
SELECT transaction_id,
       account_id,
       transaction_timestamp,
       amount,
       LAG(amount)  OVER (PARTITION BY account_id ORDER BY transaction_timestamp) AS prev_amount,
       LEAD(amount) OVER (PARTITION BY account_id ORDER BY transaction_timestamp) AS next_amount
FROM silver_transactions;

-- SUM OVER: running balance per account (unbounded preceding) --------
SELECT transaction_id,
       account_id,
       transaction_timestamp,
       amount,
       SUM(amount) OVER (
           PARTITION BY account_id
           ORDER BY transaction_timestamp
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS running_amount
FROM silver_transactions;

-- Rolling 7-day amount (Trino/Athena RANGE interval) -----------------
SELECT account_id,
       transaction_timestamp,
       amount,
       SUM(amount) OVER (
           PARTITION BY account_id
           ORDER BY transaction_timestamp
           RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
       ) AS rolling_7d_amount
FROM silver_transactions;

-- FIRST_VALUE / LAST_VALUE per account -------------------------------
SELECT DISTINCT
       account_id,
       FIRST_VALUE(amount) OVER (
           PARTITION BY account_id ORDER BY transaction_timestamp
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_amount,
       LAST_VALUE(amount) OVER (
           PARTITION BY account_id ORDER BY transaction_timestamp
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amount
FROM silver_transactions;

-- Customer spending percentile (customer segmentation) ---------------
WITH spend AS (
    SELECT account_id, SUM(amount) AS total_spend
    FROM silver_transactions
    GROUP BY account_id
)
SELECT account_id,
       total_spend,
       PERCENT_RANK() OVER (ORDER BY total_spend) AS spend_percentile,
       NTILE(4)       OVER (ORDER BY total_spend) AS spend_quartile
FROM spend;
