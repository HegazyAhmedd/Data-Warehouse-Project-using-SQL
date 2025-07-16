-- =================================================================================================
-- 📊 Data Transformation & Load Script — Bronze ➔ Silver Layer
-- -------------------------------------------------------------------------------------------------
-- 📝 Purpose:
--   This script performs the end-to-end transformation and loading of various CRM & ERP datasets
--   from the Bronze (raw) layer to the Silver (validated) layer following the Medallion Architecture.
--
-- ✅ Core Transformation Logic:
--   1️⃣ crm_cust_info:
--       - Normalize names, marital status, gender
--       - Convert create date column to DATE
--       - Deduplicate using latest record per customer ID
--
--   2️⃣ crm_prd_info:
--       - Normalize product keys and categories
--       - Map product lines to descriptive names
--       - Clean date fields and derive product end date
--
--   3️⃣ crm_sales_details:
--       - Validate and correct dates (fallback to sibling rows)
--       - Recalculate sales amount if invalid
--       - Cleanse quantities and prices
--
--   4️⃣ erp_cust_az12:
--       - Normalize customer IDs
--       - Validate age (18+ only)
--       - Map gender codes
--
--   5️⃣ erp_loc_a101:
--       - Normalize customer IDs
--       - Standardize country names
--
--   6️⃣ erp_px_cat_g1v2:
--       - Direct load (no transformation)
--
-- 🔄 All transformations applied before loading into respective Silver tables.
-- =================================================================================================


-- ================================================================
-- 🗂️ crm_cust_info - Deduplicate & Clean Customer Master Data
-- ================================================================
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),

    -- ✅ Standardize marital status codes
    -- Why: Ensure consistent labeling for business reporting
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'N/A'
    END,

    -- ✅ Standardize gender codes
    -- Why: Make gender values human-readable and consistent
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'F' THEN 'Female'
        WHEN 'M' THEN 'Male'
        ELSE 'N/A'
    END,

    cst_create_date

FROM (
    -- ✅ Deduplicate by keeping latest record per customer
    -- Why: Ensure only the most recent valid record is loaded
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY STR_TO_DATE(TRIM(cst_create_date), '%Y-%m-%d') DESC
           ) AS row_rank
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS latest_customer_records
WHERE row_rank = 1;


-- ================================================================
-- 🗂️ crm_prd_info — Normalize & Clean Product Data
-- ================================================================
TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    dwh_create_date
)
SELECT 
    prd_id,

    -- ✅ Extract category from prd_key
    -- Why: Normalize keys for category-level reporting
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),

    -- ✅ Extract product-specific key
    -- Why: Split composite keys for easier joins and queries
    SUBSTRING(prd_key, 7),

    prd_nm,

    -- ✅ Replace NULL product cost with 0
    -- Why: Prevent calculation issues downstream
    IFNULL(prd_cost, 0),

    -- ✅ Map product lines to descriptive labels
    -- Why: Business needs readable categories for analysis
    CASE UPPER(TRIM(prd_line))
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END,

    prd_start_dt,

    -- ✅ Derive product end date based on next version's start date
    -- Why: Ensure accurate product validity ranges (SCD handling)
    LEAD(prd_start_dt) OVER (
        PARTITION BY prd_nm
        ORDER BY prd_start_dt
    ) - INTERVAL 1 DAY,

    CURRENT_TIMESTAMP

FROM (
    -- ✅ Parse product dates from string
    -- Why: Convert inconsistent date strings to standard DATE format
    SELECT 
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        STR_TO_DATE(prd_start_dt, '%m/%d/%Y') AS prd_start_dt,
        STR_TO_DATE(prd_end_dt, '%m/%d/%Y') AS prd_end_dt
    FROM bronze.crm_prd_info
) AS cleaned_product_data;


-- ================================================================
-- 🗂️ crm_sales_details — Clean & Correct Sales Transactions
-- ================================================================
TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- ✅ Correct order date by fallback to sibling record if invalid
    -- Why: Handle data quality issues in source system (invalid/missing dates)
    CASE 
        WHEN LENGTH(b2.sls_order_dt) = 8 THEN STR_TO_DATE(b2.sls_order_dt, '%Y%m%d')
        ELSE (
            SELECT STR_TO_DATE(b1.sls_order_dt, '%Y%m%d')
            FROM bronze.crm_sales_details AS b1
            WHERE b1.sls_ord_num = b2.sls_ord_num 
              AND LENGTH(b1.sls_order_dt) = 8
            LIMIT 1
        )
    END,

    -- ✅ Correct ship date by fallback to sibling record if invalid
    CASE 
        WHEN LENGTH(b2.sls_ship_dt) = 8 THEN STR_TO_DATE(b2.sls_ship_dt, '%Y%m%d')
        ELSE (
            SELECT STR_TO_DATE(b1.sls_ship_dt, '%Y%m%d')
            FROM bronze.crm_sales_details AS b1
            WHERE b1.sls_ord_num = b2.sls_ord_num 
              AND LENGTH(b1.sls_ship_dt) = 8
            LIMIT 1
        )
    END,

    -- ✅ Correct due date by fallback to sibling record if invalid
    CASE 
        WHEN LENGTH(b2.sls_due_dt) = 8 THEN STR_TO_DATE(b2.sls_due_dt, '%Y%m%d')
        ELSE (
            SELECT STR_TO_DATE(b1.sls_due_dt, '%Y%m%d')
            FROM bronze.crm_sales_details AS b1
            WHERE b1.sls_ord_num = b2.sls_ord_num 
              AND LENGTH(b1.sls_due_dt) = 8
            LIMIT 1
        )
    END,

    -- ✅ Recalculate sales amount if invalid
    -- Why: Ensure sales = quantity × price if missing or wrong
    CASE 
        WHEN sls_sales <> (sls_quantity * sls_price) 
             OR sls_sales IS NULL 
             OR sls_sales <= 0 
        THEN ABS(sls_quantity) * ABS(sls_price)
        ELSE sls_sales
    END,

    -- ✅ Cleanse quantities to be positive
    ABS(sls_quantity),

    -- ✅ Correct price if invalid
    -- Why: Prevent division by zero, fallback logic applied
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN ABS(sls_sales) / NULLIF(ABS(sls_quantity), 0)
        ELSE sls_price
    END

FROM bronze.crm_sales_details AS b2;


-- ================================================================
-- 🗂️ erp_cust_az12 — Validate Age & Normalize Customer Data
-- ================================================================
TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
    CID,
    BDATE,
    GEN
)
SELECT 
    -- ✅ Normalize CID to 10 characters uppercase
    -- Why: Business system expects standardized customer IDs
    CASE  
        WHEN LENGTH(TRIM(cid)) = 13 THEN RIGHT(TRIM(UPPER(cid)), 10)
        ELSE TRIM(UPPER(cid))
    END,

    -- ✅ Nullify birthdate if customer is younger than 18
    -- Why: Business rule for compliance with adult customer records only
    CASE 
        WHEN bdate >= CURRENT_DATE() - INTERVAL 18 YEAR THEN NULL
        ELSE bdate 
    END,

    -- ✅ Standardize gender representation
    -- Why: Ensure consistency across systems
    CASE   
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    END

FROM bronze.erp_cust_az12;


-- ================================================================
-- 🗂️ erp_loc_a101 — Clean & Standardize Location Data
-- ================================================================
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
    CID,
    CNTRY
)
SELECT 
    -- ✅ Normalize CID by removing dashes
    -- Why: Standardize keys for cross-system joins
    REPLACE(cid, '-', ''),

    -- ✅ Standardize country codes to full names
    -- Why: Reporting consistency and compliance
    CASE 
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
        ELSE TRIM(cntry)
    END

FROM bronze.erp_loc_a101;


-- ================================================================
-- 🗂️ erp_px_cat_g1v2 — Direct Load of Product-Category Mapping
-- ================================================================
TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
SELECT 
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

