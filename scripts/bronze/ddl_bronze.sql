-- ===============================================================
-- 📂 File Name   : ddl_bronze.sql
-- 📦 Layer       : Bronze Layer (Raw Ingest)
-- 🏗️ Architecture: Medallion Architecture
-- 🛠️ Purpose     : Defines raw staging tables for ERP and CRM sources
-- 📝 Description :
--   This script creates raw data ingestion tables in the `bronze` schema.
--   These tables form the **first layer** of the Medallion Architecture and
--   are designed to ingest **untransformed, raw CSV data** from ERP and CRM systems.
--
--   ⚠️ Date Handling Note:
--     ▸ Date fields are intentionally defined as `VARCHAR` instead of `DATE`
--       because MySQL lacks flexible native support for validating or parsing
--       multiple input date formats during raw ingestion.
--     ▸ Actual type enforcement and format normalization are deferred
--       to the Silver layer transformations (e.g., using STR_TO_DATE).
--
--   ❖ Tables Created:
--     - bronze.erp_cust_az12        : Customer basic info from ERP
--     - bronze.erp_loc_a101         : Customer location mapping
--     - bronze.erp_px_cat_g1v2      : Product-category hierarchy
--     - bronze.crm_cust_info        : Customer profiles from CRM
--     - bronze.crm_prd_info         : Product information from CRM
--     - bronze.crm_sales_details    : Sales transactions from CRM
--
--   🧱 These tables serve as the foundation for downstream cleaning, parsing,
--      and loading into the Silver layer.
--
-- 💾 Source Format : CSV files via LOAD DATA INFILE
-- 🌐 Target Engine : MySQL
-- ===============================================================


DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    CID VARCHAR(50),
    BDATE varchar(20),
    GEN VARCHAR(15)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID VARCHAR(20),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(20)
);

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date varchar(50)
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt varchar(20),
    prd_end_dt varchar(20)
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
