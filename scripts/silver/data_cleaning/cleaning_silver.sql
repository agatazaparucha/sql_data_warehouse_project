/*********************************************************************************
    Title: Data Cleaning and Transformation Script
    Script Purpose: This script performs data cleaning and transformation on tables from the 'bronze' layer
                    to prepare them for insertion into the 'silver' layer. It addresses issues such as:
                    - Null values and invalid data.
                    - Data standardization (e.g., gender, product lines, country names).
                    - Date validation and correction.
                    - Primary key checks and duplicate removal.
                    - Logical consistency checks (e.g., sales calculations, date ranges).
    Key Features:
        - Validates and cleans data for tables: crm_prd_info, crm_sales_details, erp_cust_az12, erp_loc_a101, and erp_px_cat_g1v2.
        - Standardizes values (e.g., gender, product lines, country names).
        - Handles edge cases (e.g., invalid dates, null prices, inconsistent sales data).
        - Ensures data integrity by applying logical rules and transformations.
    Warnings:
        - This script assumes the source data in the 'bronze' layer is consistent and accessible.
        - Some transformations (e.g., sales calculations, date corrections) are based on assumptions.
        - Always validate the results of this script before loading data into the 'silver' layer.
*********************************************************************************/

/********************************************
        Table: crm_prd_info
*********************************************/

-- Check for nulls or duplicates in Primary Key
SELECT
    prd_id,
    COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;
-- No repeating values - good

-- Extract and clean product data
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category name from prd_key and replace delimiter
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- Check for nulls or negative numbers in the price column
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Replace null prices with 0
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- Standardize product line values
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Transform product line abbreviations to readable values
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- Validate date ranges: end date cannot be earlier than start date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Correct invalid end dates using the next product's start date
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CASE 
        WHEN prd_end_dt < prd_start_dt THEN CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1 AS DATE)
        ELSE CAST(prd_end_dt AS DATE)
    END AS prd_end_dt
FROM bronze.crm_prd_info;

/********************************************
        Table: crm_sales_details
*********************************************/

-- Validate and clean date columns
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_sales, 
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;

-- Check for invalid sales data
SELECT * 
FROM bronze.crm_sales_details 
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- Fix invalid sales data
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

/********************************************
        Table: erp_cust_az12
*********************************************/

-- Clean customer ID and validate birth dates
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;

/********************************************
        Table: erp_loc_a101
*********************************************/

-- Standardize country names and clean customer IDs
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM [DataWarehouse].[bronze].[erp_loc_a101];

/********************************************
        Table: erp_px_cat_g1v2
*********************************************/

-- Check for unwanted spaces and standardize data
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

-- Validate maintenance column
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
