-- ==================================================================
-- DataForge :: Advanced SQL transformation catalogue
-- CASE/COALESCE/NULLIF/CAST, date & string functions, REGEX, CTEs,
-- subqueries, and set operations (UNION / INTERSECT / EXCEPT).
-- Portable across Athena/Trino & Redshift (dialect notes inline).
-- ==================================================================

-- CASE WHEN: bucket transactions by amount ---------------------------
SELECT transaction_id,
       amount,
       CASE
           WHEN amount >= 5000 THEN 'HIGH'
           WHEN amount >= 500  THEN 'MEDIUM'
           ELSE 'LOW'
       END AS amount_band
FROM silver_transactions;

-- COALESCE / NULLIF: safe defaults & divide-by-zero guard ------------
SELECT transaction_id,
       COALESCE(currency, 'USD')            AS currency_filled,
       amount / NULLIF(fee_amount, 0)       AS amount_to_fee_ratio
FROM silver_transactions;

-- CAST: string -> typed ---------------------------------------------
SELECT CAST(amount AS DECIMAL(18,2))                    AS amount_dec,
       CAST(transaction_timestamp AS DATE)              AS txn_date,
       CAST(fraud_flag AS INTEGER)                      AS fraud_int
FROM silver_transactions;

-- DATE functions (Athena/Trino; Redshift equivalents in comments) ----
SELECT transaction_id,
       DATE_TRUNC('month', transaction_timestamp)       AS txn_month,
       EXTRACT(YEAR FROM transaction_timestamp)         AS txn_year,
       DATE_DIFF('day', DATE '2025-01-01', transaction_timestamp) AS days_since_ny  -- Redshift: DATEDIFF(day, ...)
FROM silver_transactions;

-- STRING functions + REGEX ------------------------------------------
SELECT customer_id,
       UPPER(last_name)                                 AS last_name_upper,
       TRIM(email)                                      AS email_trimmed,
       SUBSTR(phone, 1, 3)                              AS phone_prefix,
       REGEXP_LIKE(email, '^[^@]+@[^@]+\.[^@]+$')       AS email_valid  -- Redshift: email ~ '...'
FROM silver_customers;

-- CTE + subquery: customers above their segment's average spend ------
WITH customer_spend AS (
    SELECT c.customer_id, c.segment, SUM(t.amount) AS total_spend
    FROM silver_customers c
    JOIN silver_accounts a     ON a.customer_id = c.customer_id
    JOIN silver_transactions t ON t.account_id  = a.account_id
    GROUP BY c.customer_id, c.segment
),
segment_avg AS (
    SELECT segment, AVG(total_spend) AS avg_spend
    FROM customer_spend
    GROUP BY segment
)
SELECT cs.customer_id, cs.segment, cs.total_spend, sa.avg_spend
FROM customer_spend cs
JOIN segment_avg sa ON sa.segment = cs.segment
WHERE cs.total_spend > sa.avg_spend;

-- Set operations -----------------------------------------------------
SELECT country FROM silver_customers
UNION
SELECT country FROM silver_merchants;

SELECT country FROM silver_customers
UNION ALL
SELECT country FROM silver_merchants;

SELECT country FROM silver_customers
INTERSECT
SELECT country FROM silver_merchants;

-- EXCEPT (Redshift: MINUS): customer countries with no merchant
SELECT country FROM silver_customers
EXCEPT
SELECT country FROM silver_merchants;
