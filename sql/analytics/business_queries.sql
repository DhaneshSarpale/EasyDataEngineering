-- ==================================================================
-- DataForge :: Business analytics queries
-- Ready to run against the gold layer via Athena or Redshift.
-- ==================================================================

-- Top 10 customers by total spend -----------------------------------
SELECT c.customer_id, c.first_name, c.last_name,
       SUM(f.amount) AS total_spend,
       COUNT(*)      AS txn_count
FROM fact_transaction f
JOIN dim_account  a ON a.account_sk = f.account_sk
JOIN dim_customer c ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spend DESC
LIMIT 10;

-- Top merchants by revenue ------------------------------------------
SELECT m.merchant_name, m.category,
       SUM(f.amount) AS revenue,
       COUNT(*)      AS txn_count
FROM fact_transaction f
JOIN dim_merchant m ON m.merchant_sk = f.merchant_sk
GROUP BY m.merchant_name, m.category
ORDER BY revenue DESC
LIMIT 20;

-- Monthly revenue trend ---------------------------------------------
SELECT d.year, d.month,
       SUM(f.amount) AS monthly_revenue,
       COUNT(*)      AS txn_count
FROM fact_transaction f
JOIN dim_date d ON d.date_sk = f.date_sk
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Average transaction value by channel ------------------------------
SELECT channel,
       ROUND(AVG(amount), 2) AS avg_txn_value,
       COUNT(*)              AS txn_count
FROM fact_transaction
GROUP BY channel
ORDER BY avg_txn_value DESC;

-- Fraud rate (by channel) -------------------------------------------
SELECT channel,
       SUM(fraud_flag)                                  AS fraud_txns,
       COUNT(*)                                         AS total_txns,
       ROUND(100.0 * SUM(fraud_flag) / COUNT(*), 3)     AS fraud_rate_pct
FROM fact_transaction
GROUP BY channel
ORDER BY fraud_rate_pct DESC;

-- Customer Lifetime Value (simple) ----------------------------------
SELECT c.customer_id,
       SUM(f.amount)                                    AS lifetime_spend,
       COUNT(*)                                         AS lifetime_txns,
       SUM(f.amount) / NULLIF(COUNT(DISTINCT d.month), 0) AS avg_monthly_value
FROM fact_transaction f
JOIN dim_account  a ON a.account_sk = f.account_sk
JOIN dim_customer c ON c.customer_id = a.customer_id
JOIN dim_date     d ON d.date_sk     = f.date_sk
GROUP BY c.customer_id
ORDER BY lifetime_spend DESC;

-- Inactive accounts (no transactions in the last 90 days) -----------
SELECT a.account_id, a.customer_id, MAX(d.date) AS last_txn_date
FROM dim_account a
LEFT JOIN fact_transaction f ON f.account_sk = a.account_sk
LEFT JOIN dim_date d         ON d.date_sk    = f.date_sk
GROUP BY a.account_id, a.customer_id
HAVING MAX(d.date) IS NULL
    OR MAX(d.date) < DATE_ADD('day', -90, CURRENT_DATE);   -- Redshift: DATEADD(day,-90,CURRENT_DATE)

-- High-value transactions (>= 5000) ---------------------------------
SELECT transaction_id, account_sk, amount, currency, channel
FROM fact_transaction
WHERE amount >= 5000
ORDER BY amount DESC;
