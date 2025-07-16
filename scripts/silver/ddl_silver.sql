-- =================================================================================================
-- 📄 File Name   : ddl_silver.sql
-- 🗂️ Layer       : Silver Layer — Refined Data Store (Medallion Architecture)
-- 🏗️ Purpose     : Define Silver Layer Tables in MySQL
-- -------------------------------------------------------------------------------------------------
-- 📝 Description :
--   This script creates the **Silver Layer** tables in the Medallion Architecture.
--   These tables store *cleansed*, *typed*, and *validated* data transformed from the Bronze layer.
--
-- ✅ Core Design Principles:
--   - Enforced data typing (especially dates)
--   - Primary key constraints where applicable
--   - Data ingestion tracking via `dwh_create_date`
--   - Aligned with Medallion Data Modeling Standards (Bronze ➔ Silver ➔ Gold)
--
-- 🧩 Tables Defined:
--   • silver.crm_cust_info      — CRM Customer Profiles
--   • silver.crm_prd_info       — CRM Product Master
--   • silver.crm_sales_details  — CRM Sales Transactions
--   • silver.erp_cust_az12      — ERP Customers Birth Date
--   • silver.erp_loc_a101       — ERP Customer Location Data
--   • silver.erp_px_cat_g1v2    — ERP Product Category Hierarchy
--
-- 🔄 Notes:
--   • Dates must be normalized **before** insertion using ETL logic.
--   • These tables are considered "trusted" for BI & Analytics consumption.
--   • Always track inserts with `dwh_create_date` for lineage and debugging.
--
-- 🌐 Target Platform: MySQL
-- =================================================================================================


-- =================================================================================================
-- 📊 CRM Customer Info Table — silver.crm_cust_info
-- -------------------------------------------------------------------------------------------------
-- ✅ Holds deduplicated CRM customer master data with cleaned attributes.
-- ✅ Supports audit by tracking creation date in DWH.
-- =================================================================================================
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- =================================================================================================
-- 📦 CRM Product Info Table — silver.crm_prd_info
-- -------------------------------------------------------------------------------------------------
-- ✅ Stores validated CRM product catalog with business categories and date ranges.
-- ✅ Product End Date is calculated during transformation based on business logic.
-- =================================================================================================
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id           INT,
    cat_id           VARCHAR(50),
    prd_key          VARCHAR(50),
    prd_nm           VARCHAR(50),
    prd_cost         INT,
    prd_line         VARCHAR(50),
    prd_start_dt     DATE,
    prd_end_dt       DATE,
    dwh_create_date  DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- =================================================================================================
-- 💰 CRM Sales Details Table — silver.crm_sales_details
-- -------------------------------------------------------------------------------------------------
-- ✅ Captures transactional CRM sales records with strict typing.
-- ✅ Dates are normalized and sales metrics validated before insert.
-- =================================================================================================
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num      VARCHAR(50),
    sls_prd_key      VARCHAR(50),
    sls_cust_id      INT,
    sls_order_dt     DATE,
    sls_ship_dt      DATE,
    sls_due_dt       DATE,
    sls_sales        INT,
    sls_quantity     INT,
    sls_price        INT,
    dwh_create_date  DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- =================================================================================================
-- 🧑‍💼 ERP Customer Master Table — silver.erp_cust_az12
-- -------------------------------------------------------------------------------------------------
-- ✅ Stores ERP customer master data after ID normalization and age verification.
-- ✅ Gender codes mapped to standardized values.
-- =================================================================================================
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    CID              VARCHAR(30),
    BDATE            DATE,
    GEN              VARCHAR(15),
    dwh_create_date  DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- =================================================================================================
-- 🌍 ERP Customer Location Table — silver.erp_loc_a101
-- -------------------------------------------------------------------------------------------------
-- ✅ Stores normalized customer location with country name standardization.
-- ✅ CID is cleansed (e.g., dashes removed) before insertion.
-- =================================================================================================
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    CID              VARCHAR(50),
    CNTRY            VARCHAR(50),
    dwh_create_date  DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- =================================================================================================
-- 🗂️ ERP Product Category Mapping Table — silver.erp_px_cat_g1v2
-- -------------------------------------------------------------------------------------------------
-- ✅ Stores ERP product category hierarchy.
-- ✅ No transformation applied; direct Bronze ➔ Silver load.
-- =================================================================================================
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    ID               VARCHAR(20),
    CAT              VARCHAR(50),
    SUBCAT           VARCHAR(50),
    MAINTENANCE      VARCHAR(20),
    dwh_create_date  DATETIME DEFAULT CURRENT_TIMESTAMP
);
