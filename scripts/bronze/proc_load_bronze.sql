/*********************************************************************************
    Title: load_bronze procedure
    Script Purpose: This stored procedure loads data into the bronze layer of the data warehouse.
                   It truncates and reloads data from CSV files into the respective tables in the 'bronze' schema.
                   Tables include CRM and ERP-related data.
    Warnings: 
        - This procedure will TRUNCATE existing tables before loading new data, which will result in data loss.
        - Ensure the CSV file paths are correct and accessible before running this procedure.
        - The procedure includes error handling to log any issues during the load process.
*********************************************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT '==================================';
        PRINT 'Loading Bronze Layer';
        PRINT '==================================';

        PRINT '==================================';
        PRINT 'Loading CRM Tables';
        PRINT '==================================';

        -- Load bronze.crm_cust_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'j:\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, -- we have headers in the first row
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.crm_cust_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------';

        -- Load bronze.crm_prd_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'j:\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_prd.csv'
        WITH (
            FIRSTROW = 2, -- we have headers in the first row
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.crm_prd_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------';

        -- Load bronze.crm_sales_details
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'j:\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2, -- we have headers in the first row
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.crm_sales_details Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------';

        PRINT '==================================';
        PRINT 'Loading ERP Tables';
        PRINT '==================================';

        -- Load bronze.erp_cust_az12
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'j:\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2, -- we have headers in the first row
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.erp_cust_az12 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------';

        -- Load bronze.erp_loc_a101
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'j:\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2, -- we have headers in the first row
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.erp_loc_a101 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------';

        -- Load bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'j:\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2, -- we have headers in the first row
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.erp_px_cat_g1v2 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
        PRINT '-------------------';

        PRINT '==================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT '==================================';
    END TRY
    BEGIN CATCH
        PRINT '==================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '==================================';
    END CATCH
END;
GO
