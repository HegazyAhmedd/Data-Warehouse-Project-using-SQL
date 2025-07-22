/* ===============================================================
   SILVER LAYER DATA QUALITY CHECKS
   ---------------------------------------------------------------
   1. Primary Key Integrity:
      - crm_prd_info.prd_id, crm_cust_info.cst_id, erp_cust_az12.cid

   2. Unwanted Spaces:
      - crm_prd_info.prd_nm, crm_cust_info.cst_key
      - erp_px_cat_g1v2.cat, subcat, maintenance

   3. Domain & Consistency:
      - crm_prd_info.prd_line, crm_cust_info.cst_marital_status
      - erp_loc_a101.cid length, cntry
      - erp_px_cat_g1v2.cat, subcat, maintenance

   4. Date Validation:
      - crm_prd_info: prd_start_dt vs prd_end_dt
      - crm_sales_details: valid dates & date order
      - erp_cust_az12: bdate range check

   5. Business Rules:
      - crm_sales_details: sls_sales = sls_quantity × sls_price

   6. Distribution Check:
      - erp_cust_az12: gen distribution
   ---------------------------------------------------------------
*/


-- ============================
-- CRM Product Info Checks
-- ============================
-- Primary Key Nulls/Duplicates
SELECT prd_id, COUNT(*) FROM silver.crm_prd_info 
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Unwanted Spaces in Product Name
SELECT * FROM silver.crm_prd_info 
WHERE prd_nm <> TRIM(prd_nm);

-- Nulls or Negative Cost
SELECT * FROM silver.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Product Line Consistency Check
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Invalid Date Orders
SELECT * FROM silver.crm_prd_info 
WHERE prd_start_dt > prd_end_dt;


-- ============================
-- CRM Customer Info Checks
-- ============================
-- Primary Key Nulls/Duplicates
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) <> 1 OR cst_id IS NULL;

-- Unwanted Spaces in Customer Key
SELECT cst_key FROM silver.crm_cust_info 
WHERE cst_key <> TRIM(cst_key);

-- Marital Status Domain Check
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;


-- ============================
-- CRM Sales Details Checks
-- ============================
-- Invalid Dates (Format/Negative)
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM silver.crm_sales_details 
WHERE sls_due_dt <= 0 OR LENGTH(sls_due_dt) <> 8;

-- Invalid Date Orders
SELECT * FROM silver.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Sales Consistency: sales = quantity * price
SELECT DISTINCT sls_sales, sls_quantity, sls_price 
FROM silver.crm_sales_details 
WHERE sls_sales != sls_quantity * sls_price 
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL 
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;


-- ============================
-- ERP Customer AZ12 Checks
-- ============================
-- Primary Key Uniqueness
SELECT cid, COUNT(*) FROM silver.erp_cust_az12 
GROUP BY cid 
HAVING COUNT(*) <> 1;

-- CID Length Consistency
SELECT DISTINCT LENGTH(cid) FROM silver.erp_cust_az12;

-- Min & Max Birth Dates Check
SELECT MIN(bdate) AS max_birth_date, MAX(bdate) AS min_birth_date 
FROM silver.erp_cust_az12;


-- Gender Distribution
SELECT gen, COUNT(*) FROM silver.erp_cust_az12 
GROUP BY gen;


-- ============================
-- ERP Location A101 Checks
-- ============================
-- CID Length Consistency
SELECT DISTINCT LENGTH(cid) FROM silver.erp_loc_a101;

-- Country Domain Check
SELECT DISTINCT cntry FROM silver.erp_loc_a101;


-- ============================
-- ERP PX Cat G1V2 Checks
-- ============================
-- Unwanted Spaces
SELECT * FROM silver.erp_px_cat_g1v2 
WHERE TRIM(cat) <> cat OR TRIM(subcat) <> subcat OR TRIM(maintenance) <> maintenance;

-- Category Domain Checks
SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;

-- ====================================================================
-- End of Quality Check Script
-- ====================================================================
