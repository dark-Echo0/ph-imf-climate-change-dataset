/*
=================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncate the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameter or return any values.

Before running this script make sure the '.csv' file format was converted to '.txt' format.
Then, I add current stamptime of this table in the last column by running this script:
		ALTER TABLE bronze.ghg_co2_emissions
		ADD dwh_create_date DATETIME2 NOT NULL DEFAULT GETDATE();

Usage Example:
    EXEC bronze.load_bronze;   
=================================================================
*/
EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.ghg_co2_emissions';
		TRUNCATE TABLE bronze.ghg_co2_emissions; 
		PRINT '>> Insert Table: bronze.ghg_co2_emissions';
		BULK INSERT bronze.ghg_co2_emissions
		FROM "C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\co2_eim.txt"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '\t',
			CODEPAGE = '65001',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';


		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==============================================';
	END TRY

	-- Script in case of Error in the Bronze Layer
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONCE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';

	END CATCH
END
