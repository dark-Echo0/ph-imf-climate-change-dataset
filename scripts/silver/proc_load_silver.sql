/*
=================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transfer, Load) process 
    to populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
    - Truncate Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameter or return any values.

Usage Example:
    EXEC silver.load_silver;
=================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.ghg_co2_emissions;'
		TRUNCATE TABLE silver.ghg_co2_emissions;
		PRINT '>> Inserting Data Into: silver.ghg_co2_emissions';
		INSERT INTO silver.ghg_co2_emissions(
		cts_name, unit, cts_code, industry,	
    [2000],	[2001],	[2002],	[2003],	[2004],	[2005],	[2006],[2007],	[2008],	[2009],	
    [2010],	[2011],	[2012],	[2013],	[2014],	[2015],	[2016],	[2017],	[2018]
		)
    SELECT 
    TRIM(cts_name) AS cts_name,
    TRIM(unit) AS unit,
    TRIM(cts_code) AS cts_code,
    REPLACE(industry, '"','') AS industry,	
    CAST([2000] AS FLOAT) AS [2000],	CAST([2001] AS FLOAT) AS [2001],	CAST([2002] AS FLOAT) AS [2002],	CAST([2003] AS FLOAT) AS [2003],	CAST([2004] AS FLOAT) AS [2004],	
    CAST([2005] AS FLOAT) AS [2005],	CAST([2006] AS FLOAT) AS [2006],    CAST([2007] AS FLOAT) AS [2007],	CAST([2008] AS FLOAT) AS [2008],	CAST([2009] AS FLOAT) AS [2010],	
    CAST([2010] AS FLOAT) AS [2010],	CAST([2011] AS FLOAT) AS [2011],	CAST([2012] AS FLOAT) AS [2012],	CAST([2013] AS FLOAT) AS [2013],	CAST([2014] AS FLOAT) AS [2014],	
    CAST([2015] AS FLOAT) AS [2015],	CAST([2016] AS FLOAT) AS [2016],	CAST([2017] AS FLOAT) AS [2017],	CAST([2018] AS FLOAT) AS [2018]
    
    FROM bronze.ghg_co2_emissions

      
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';
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










