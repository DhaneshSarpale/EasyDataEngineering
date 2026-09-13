-- ==================================================================
-- DataForge :: Slowly Changing Dimensions in SQL
-- SCD Type 1 (overwrite) and SCD Type 2 (versioned history).
-- Redshift-flavoured (MERGE); Athena is read-only so SCD there is done
-- via CTAS + INSERT/OVERWRITE partitions.
-- ==================================================================

-- ------------------------------------------------------------------
-- SCD Type 1: overwrite. No history retained.
-- ------------------------------------------------------------------
MERGE INTO dim_customer_scd1 AS tgt
USING stg_customers AS src
    ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
    first_name = src.first_name,
    last_name  = src.last_name,
    city       = src.city,
    country    = src.country,
    segment    = src.segment
WHEN NOT MATCHED THEN INSERT
    (customer_id, first_name, last_name, city, country, segment)
    VALUES (src.customer_id, src.first_name, src.last_name,
            src.city, src.country, src.segment);


-- ------------------------------------------------------------------
-- SCD Type 2: versioned history.
--   customer_sk (surrogate), customer_id (natural),
--   address, city, country,
--   effective_from, effective_to, is_current
-- ------------------------------------------------------------------

-- Step 1: expire the current version for changed rows.
UPDATE dim_customer_scd2 AS tgt
SET effective_to = src.updated_at,
    is_current   = FALSE
FROM stg_customers AS src
WHERE tgt.customer_id = src.customer_id
  AND tgt.is_current  = TRUE
  AND (tgt.city    <> src.city
    OR tgt.country <> src.country
    OR tgt.address <> src.address);

-- Step 2: insert the new current version for new + changed rows.
INSERT INTO dim_customer_scd2
    (customer_sk, customer_id, address, city, country,
     effective_from, effective_to, is_current)
SELECT
    MD5(src.customer_id || '|' || CAST(src.updated_at AS VARCHAR)) AS customer_sk,
    src.customer_id,
    src.address, src.city, src.country,
    src.updated_at                       AS effective_from,
    CAST('9999-12-31' AS TIMESTAMP)      AS effective_to,
    TRUE                                 AS is_current
FROM stg_customers src
LEFT JOIN dim_customer_scd2 cur
       ON cur.customer_id = src.customer_id
      AND cur.is_current  = TRUE
WHERE cur.customer_id IS NULL
   OR cur.city    <> src.city
   OR cur.country <> src.country
   OR cur.address <> src.address;

-- Query the dimension "as of" a point in time (temporal join) --------
-- SELECT * FROM dim_customer_scd2
-- WHERE customer_id = 'CUST00000042'
--   AND TIMESTAMP '2025-06-15' >= effective_from
--   AND TIMESTAMP '2025-06-15' <  effective_to;
