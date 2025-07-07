-- ===============================================================
-- 📂 File Name   : load_bronze_raw_data.sql
-- 📦 Layer       : Bronze Layer (Raw Ingest)
-- 🏗️ Architecture: Medallion Architecture
-- 🛠️ Purpose     : Load raw data from CSV files into Bronze layer tables
--
-- 📝 Description :
--   This script performs raw data ingestion into the Bronze layer 
--   of the Medallion Architecture using MySQL.
--
--   It loads data from CSV source files (ERP & CRM) into staging tables
--   after truncating existing data, ensuring each load is clean.
--
--   🚨 Empty CSV fields are safely converted to NULL using user variables
--   and NULLIF() — this prevents incorrect default values like 0 or ''.
--
--   ❖ Tables Affected:
--     1. bronze.erp_cust_az12        : ERP customer info
--     2. bronze.erp_loc_a101         : ERP customer-country map
--     3. bronze.erp_px_cat_g1v2      : Product category hierarchy
--     4. bronze.crm_prd_info         : CRM product master data
--     5. bronze.crm_cust_info        : CRM customer profiles
--     6. bronze.crm_sales_details    : CRM sales transaction records
--
-- 💾 File Location:
--   All CSV files must be stored in:
--   C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/
--
-- 🔧 Requirements:
--   - SET GLOBAL local_infile = 1;
--   - SET SESSION sql_mode = '';
--   - Ensure MySQL has read access to the CSV folder
--
-- 🛑 Notes:
--   - LOAD DATA INFILE is not permitted inside stored procedures
--   - This script must be executed as a standalone batch
--   - NULL handling is applied using @vars + NULLIF to avoid data errors
--
-- ✅ Tested Environment:
--   - MySQL Server 8.0.x
--   - Windows OS with local_infile enabled
--
-- 📅 Last Updated: [Insert Date Here]
-- 👤 Author: [Your Name]
-- ===============================================================


-- Table 1: bronze.erp_cust_az12
TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@CID, @BDATE, @GEN)
SET CID = NULLIF(@CID, ''),
    BDATE = NULLIF(@BDATE, ''),
    GEN = NULLIF(@GEN, '');

-- ========================================
-- Table 2: bronze.erp_loc_a101
TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@CID, @CNTRY)
SET CID = NULLIF(@CID, ''),
    CNTRY = NULLIF(@CNTRY, '');

-- ========================================
-- Table 3: bronze.erp_px_cat_g1v2
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@ID, @CAT, @SUBCAT, @MAINTENANCE)
SET ID = NULLIF(@ID, ''),
    CAT = NULLIF(@CAT, ''),
    SUBCAT = NULLIF(@SUBCAT, ''),
    MAINTENANCE = NULLIF(@MAINTENANCE, '');

-- ========================================
-- Table 4: bronze.crm_prd_info
TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
SET prd_id = NULLIF(@prd_id, ''),
    prd_key = NULLIF(@prd_key, ''),
    prd_nm = NULLIF(@prd_nm, ''),
    prd_cost = NULLIF(@prd_cost, ''),
    prd_line = NULLIF(@prd_line, ''),
    prd_start_dt = NULLIF(@prd_start_dt, ''),
    prd_end_dt = NULLIF(@prd_end_dt, '');

-- ========================================
-- Table 5: bronze.crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET cst_id = NULLIF(@cst_id, ''),
    cst_key = NULLIF(@cst_key, ''),
    cst_firstname = NULLIF(@cst_firstname, ''),
    cst_lastname = NULLIF(@cst_lastname, ''),
    cst_marital_status = NULLIF(@cst_marital_status, ''),
    cst_gndr = NULLIF(@cst_gndr, ''),
    cst_create_date = NULLIF(@cst_create_date, '');

-- ========================================
-- Table 6: bronze.crm_sales_details
TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
SET sls_ord_num   = NULLIF(@sls_ord_num, ''),
    sls_prd_key   = NULLIF(@sls_prd_key, ''),
    sls_cust_id   = NULLIF(@sls_cust_id, ''),
    sls_order_dt  = NULLIF(@sls_order_dt, ''),
    sls_ship_dt   = NULLIF(@sls_ship_dt, ''),
    sls_due_dt    = NULLIF(@sls_due_dt, ''),
    sls_sales     = NULLIF(@sls_sales, ''),
    sls_quantity  = NULLIF(@sls_quantity, ''),
    sls_price     = NULLIF(@sls_price, '');
