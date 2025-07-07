-- ===============================================================
-- 📂 File Name   : load_bronze_raw_data.sql
-- 📦 Layer       : Bronze Layer (Raw Ingest)
-- 🏗️ Architecture: Medallion Architecture
-- 🛠️ Purpose     : Load raw data from CSV files into Bronze layer tables
--
-- 📝 Description :
--   This script performs raw data ingestion into the Bronze layer 
--   as part of a Medallion Architecture implemented in MySQL.
--
--   ❖ For each table:
--     - Truncates existing data
--     - Loads fresh data using LOAD DATA INFILE
--     - Assumes CSV format with headers and double-quote encapsulation
--
--   ❖ Tables Affected:
--     1. bronze.erp_cust_az12        : ERP customer info
--     2. bronze.erp_loc_a101         : ERP location codes
--     3. bronze.erp_px_cat_g1v2      : Product category mapping
--     4. bronze.crm_prd_info         : CRM product master
--     5. bronze.crm_cust_info        : CRM customer profiles
--     6. bronze.crm_sales_details    : CRM transactional sales
--
-- 💾 File Location:
--   CSV files must be placed in the following directory:
--   C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/
--   (required by MySQL's LOAD DATA INFILE for local loading)
--
-- ⚠️ Requirements:
--   - SET GLOBAL local_infile = 1;
--   - SET SESSION sql_mode = '';
--   - Appropriate file system permissions
--
-- 🔍 Execution Notes:
--   - LOAD DATA INFILE is not allowed inside stored procedures
--   - This script is intended for standalone execution
-- ===============================================================


-- Table 1: bronze.erp_cust_az12
TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(CID, BDATE, GEN);

-- ========================================
-- Table 2: bronze.erp_loc_a101
TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(CID, CNTRY);

-- ========================================
-- Table 3: bronze.erp_px_cat_g1v2
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(ID, CAT, SUBCAT, MAINTENANCE);

-- ========================================
-- Table 4: bronze.crm_prd_info
TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);

-- ========================================
-- Table 5: bronze.crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);

-- ========================================
-- Table 6: bronze.crm_sales_details
TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);



