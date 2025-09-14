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
CREATE OR ALTER PROCEDURE bronze.load_bronze_ghg AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		-- Create bronze.ghg_co2_emissions
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.ghg_co2_emissions';
		TRUNCATE TABLE bronze.ghg_co2_emissions; 
		PRINT '>> Insert Table: bronze.ghg_co2_emissions';
		BULK INSERT bronze.ghg_co2_emissions
		FROM 'C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\04_CO2_Emissions_Emissions_Intensities_and_Emissions_Multipliers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			KEEPNULLS,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		-- Create bronze.ghg_carbon_footprints
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.ghg_carbon_footprints';
		TRUNCATE TABLE bronze.ghg_carbon_footprints; 
		PRINT '>> Insert Table: bronze.ghg_carbon_footprints';
		BULK INSERT bronze.ghg_carbon_footprints
		FROM 'C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\05_CO2_Emissions_embodied_in_Domestic_Final_Demand_Production_and_Trade.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			KEEPNULLS,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		-- Create 'bronze.ghg_investment_related' 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.ghg_investment_related';
		TRUNCATE TABLE bronze.ghg_investment_related; 
		PRINT '>> Insert Table: bronze.ghg_investment_related';
		BULK INSERT bronze.ghg_investment_related
		FROM 'C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\06_Direct_Investment-related_Indicators.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			KEEPNULLS,
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
