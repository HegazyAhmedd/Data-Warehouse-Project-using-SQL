-- ====================================================================
-- 📂 File      : create_gold_layer_views.sql
-- 📊 Purpose   : Create Gold Layer Views for Direct Analytics & Queries
-- 📝 Description:
--   - Builds dimensional and fact views optimized for BI tools and reports
--   - Leverages surrogate keys for consistent joins across dimensions/facts
--   - Transforms raw Silver layer data into analytics-ready datasets
--
-- 📦 Included Views:
--   1. gold.dim_customers → Combines CRM & ERP customer info with location
--   2. gold.dim_products  → Merges CRM product data with ERP category mapping
--   3. gold.fact_sales    → Transactional sales fact table linked to dimensions
--
-- 🛠 Business Logic Highlights:
--   - Surrogate keys created with ROW_NUMBER for each dimension
--   - Gender prioritized from CRM, fallback to ERP, else 'N/A'
--   - Products filtered to only latest active version (prd_end_dt IS NULL)
--   - Fact table joins product & customer dimensions for complete context
--
-- 🎯 Use Case:
--   - Designed for direct querying by analysts
--   - Ready to connect to visualization tools (Power BI, Tableau, Looker)
-- ====================================================================

DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT 
    -- ✅ Surrogate key for dimensional modeling
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    c1.cst_id          AS customer_id,
    c1.cst_key         AS customer_code,
    c1.cst_firstname   AS first_name,
    c1.cst_lastname    AS last_name,

    l.cntry            AS country,

    -- ✅ Gender prioritization logic:
    -- 1. Use CRM gender if it's NOT 'N/A' it's the master source
    -- 2. Else use ERP gender if it's NOT 'N/A'
    -- 3. Else default to 'N/A'
    COALESCE(NULLIF(c1.cst_gndr, 'N/A'), NULLIF(c2.gen, 'N/A'), 'N/A') AS gender,

    c1.cst_marital_status AS marital_status,

    IFNULL(c2.bdate, 'N/A') AS birth_date,

    c1.cst_create_date AS create_date

FROM silver.crm_cust_info c1
LEFT JOIN silver.erp_cust_az12 c2 ON c1.cst_key = c2.cid
LEFT JOIN silver.erp_loc_a101 l   ON c1.cst_key = l.cid;

-- =====================================================================

DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key, -- Surrogate key for DWH
    prd_id            AS product_id,         -- Business key from CRM
    prd_key           AS product_code,
    prd_nm            AS product_name,
    cat_id            AS category_id,
    cat               AS category,
    subcat            AS subcategory,
    maintenance,
    prd_cost          AS cost,
    prd_line          AS product_line,
    prd_start_dt      AS start_date
FROM 
    silver.crm_prd_info p1
LEFT JOIN 
    silver.erp_px_cat_g1v2 p2 
    ON p1.cat_id = p2.id
WHERE 
    prd_end_dt IS NULL; -- Keep only the latest active product version

-- ==================================================================== 
 
DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT 
    S.sls_ord_num                       AS order_number,      -- Sales order number (transaction identifier)
    P.product_key                        AS product_key,       -- Surrogate key from dim_products
    C.customer_key                       AS customer_key,      -- Surrogate key from dim_customers
    S.sls_order_dt                       AS order_date,        -- Date order was placed
    S.sls_ship_dt                        AS ship_date,         -- Date order was shipped
    S.sls_due_dt                         AS due_date,          -- Date payment/delivery is due
    S.sls_sales                          AS sales_amount,      -- Total sales amount for the line
    S.sls_quantity                       AS quantity_sold,     -- Number of units sold
    S.sls_price                          AS unit_price         -- Price per unit
FROM 
    silver.crm_sales_details S
LEFT JOIN 
    gold.dim_customers C 
    ON S.sls_cust_id = C.customer_id
LEFT JOIN 
    gold.dim_products P 
    ON S.sls_prd_key = P.product_code;
 