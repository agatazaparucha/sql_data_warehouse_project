/*********************************************************************************
    Title for file: Create_DataWarehouse_Database.sql
    Script Purpose: This script checks if the 'DataWarehouse' database exists, drops it if it does,
                   and then creates a new 'DataWarehouse' database with three schemas: gold, silver, and bronze.
    Warnings: 
        - This script will DROP the existing 'DataWarehouse' database if it exists, which will result in data loss.
        - Ensure no critical applications or users are connected to the database before running this script.
        - The script uses SINGLE_USER mode with ROLLBACK IMMEDIATE, which will forcefully disconnect all active sessions.
*********************************************************************************/
USE master;
GO

-- check if DB exists before creating
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA gold;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA bronze;
GO
