/*
===============================================================================
FACT-DIM QUALITY CHECKS — EXPECTATION: ZERO FAILURES
===============================================================================
Script Purpose:
    This script validates the integrity, consistency, and accuracy of the
    Gold Layer data model by running key quality checks:
    - Ensure uniqueness of surrogate keys in dimension tables.
    - Validate referential integrity between fact and dimension tables.
    - Confirm correct relationships for analytics readiness.

Usage Notes:
    - All checks should return zero failures.
    - Investigate and fix any records that fail the checks before proceeding.
===============================================================================
*/


-- 1️⃣ Check for duplicate primary Customer Keys in dim_customers
-- Expectation: 0 rows 
SELECT customer_key, COUNT(*) AS cnt
FROM dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- 2️⃣ Check for primary Product Keys in dim_products
-- Expectation: 0 rows 
SELECT product_key, COUNT(*) AS cnt
FROM dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- This ensures every product_key in fact_sales exists in dim_products
-- Expectation: 0 rows 
SELECT *
FROM fact_sales AS S
LEFT JOIN dim_products AS P 
    ON S.product_key = P.product_key
WHERE P.product_key IS NULL;

-- This ensures every customer_key in fact_sales exists in dim_customers
-- Expectation: 0 rows 
SELECT *
FROM fact_sales AS S
LEFT JOIN dim_customers AS C
    ON S.customer_key = C.customer_key
WHERE C.customer_key IS NULL;
