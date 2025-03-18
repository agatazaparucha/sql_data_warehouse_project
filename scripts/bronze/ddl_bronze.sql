/*********************************************************************************
    Title: Create Bronze Tables
    Script Purpose: This script checks if tables in the 'bronze' schema exist, drops them if they do,
                   and then creates new tables for the bronze layer of the data warehouse.
                   Tables include CRM and ERP-related data.
    Warnings: 
        - This script will DROP existing tables if they exist, which will result in data loss.
        - Ensure no critical applications or users are connected to the database before running this script.
        - The script is designed to be idempotent, meaning it can be run multiple times without errors.
*********************************************************************************/

-- Check if 'bronze.crm_cust_info' exists and drop it if it does
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

-- Create 'bronze.crm_cust_info' table
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO

-- Check if 'bronze.crm_prd_info' exists and drop it if it does
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

-- Create 'bronze.crm_prd_info' table
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO

-- Check if 'bronze.crm_sales_details' exists and drop it if it does
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

-- Create 'bronze.crm_sales_details' table
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO

-- Check if 'bronze.erp_loc_a101' exists and drop it if it does
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

-- Create 'bronze.erp_loc_a101' table
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- Check if 'bronze.erp_cust_az12' exists and drop it if it does
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

-- Create 'bronze.erp_cust_az12' table
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);
GO

-- Check if 'bronze.erp_px_cat_g1v2' exists and drop it if it does
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

-- Create 'bronze.erp_px_cat_g1v2' table
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO
