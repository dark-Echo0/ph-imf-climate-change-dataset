/*
**********************************************************************************
DDL Script: Silver Table
**********************************************************************************
Script Purpose:
    This scripts creates tables in the 'silver' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' tables
**********************************************************************************
*/
-- create 'silver.ghg_co2_emissions' table
IF OBJECT_ID('silver.ghg_co2_emissions', 'U') IS NOT NUll
	DROP TABLE silver.ghg_co2_emissions; 
  
SELECT
  country,	iso2,	iso3,	indicator_used, unit, 
  cts_code,	industry,
  CAST([1995] AS FLOAT) AS [1995],	CAST([1996] AS FLOAT) AS [1996],	CAST([1997] AS FLOAT) AS [1997],	CAST([1998] AS FLOAT) AS [1998],	CAST([1999] AS FLOAT) AS [1999],	CAST([2000] AS FLOAT) AS [2000],	
  CAST([2001] AS FLOAT) AS [2001],	CAST([2002] AS FLOAT) AS [2002],	CAST([2003] AS FLOAT) AS [2003],	CAST([2004] AS FLOAT) AS [2004],	CAST([2005] AS FLOAT) AS [2005],	CAST([2006] AS FLOAT) AS [2006],
  CAST([2007] AS FLOAT) AS [2007],	CAST([2008] AS FLOAT) AS [2008],	CAST([2009] AS FLOAT) AS [2009],	CAST([2010] AS FLOAT) AS [2010],	CAST([2011] AS FLOAT) AS [2011],	CAST([2012] AS FLOAT) AS [2012],	
  CAST([2013] AS FLOAT) AS [2013],	CAST([2014] AS FLOAT) AS [2014],	CAST([2015] AS FLOAT) AS [2015],	CAST([2016] AS FLOAT) AS [2016],	CAST([2017] AS FLOAT) AS [2017],	CAST([2018] AS FLOAT) AS [2018]
INTO silver.ghg_co2_emissions
FROM bronze.ghg_co2_emissions
GO

-- Create 'silver.ghg_carbon_footprints' table
IF OBJECT_ID('silver.ghg_carbon_footprints', 'U') IS NOT NUll
	DROP TABLE silver.ghg_carbon_footprints; 

SELECT
  country,	iso2,	iso3,	indicator_used, 
  unit, cts_code,
  CAST([1995] AS FLOAT) AS [1995],	CAST([1996] AS FLOAT) AS [1996],	CAST([1997] AS FLOAT) AS [1997],	CAST([1998] AS FLOAT) AS [1998],	CAST([1999] AS FLOAT) AS [1999],	CAST([2000] AS FLOAT) AS [2000],	
  CAST([2001] AS FLOAT) AS [2001],	CAST([2002] AS FLOAT) AS [2002],	CAST([2003] AS FLOAT) AS [2003],	CAST([2004] AS FLOAT) AS [2004],	CAST([2005] AS FLOAT) AS [2005],	CAST([2006] AS FLOAT) AS [2006],
  CAST([2007] AS FLOAT) AS [2007],	CAST([2008] AS FLOAT) AS [2008],	CAST([2009] AS FLOAT) AS [2009],	CAST([2010] AS FLOAT) AS [2010],	CAST([2011] AS FLOAT) AS [2011],	CAST([2012] AS FLOAT) AS [2012],	
  CAST([2013] AS FLOAT) AS [2013],	CAST([2014] AS FLOAT) AS [2014],	CAST([2015] AS FLOAT) AS [2015],	CAST([2016] AS FLOAT) AS [2016],	CAST([2017] AS FLOAT) AS [2017],	CAST([2018] AS FLOAT) AS [2018],
  CAST([2019] AS FLOAT) AS [2019],  CAST([2020] AS FLOAT) AS [2020],    CAST([2021] AS FLOAT) AS [2021]
INTO silver.ghg_carbon_footprints
FROM bronze.ghg_carbon_footprints

-- Create 'silver.ghg_investment_related' table
IF OBJECT_ID('silver.ghg_investment_related', 'U') IS NOT NUll
	DROP TABLE silver.ghg_investment_related; 
  
SELECT
  country,	iso2,	iso3,	indicator_used, unit, cts_code, sector,	
  CAST([2005] AS FLOAT) AS [2005],	CAST([2006] AS FLOAT) AS [2006], CAST([2007] AS FLOAT) AS [2007],	CAST([2008] AS FLOAT) AS [2008],	CAST([2009] AS FLOAT) AS [2009],	CAST([2010] AS FLOAT) AS [2010],	
  CAST([2011] AS FLOAT) AS [2011],	CAST([2012] AS FLOAT) AS [2012], CAST([2013] AS FLOAT) AS [2013],	CAST([2014] AS FLOAT) AS [2014],	CAST([2015] AS FLOAT) AS [2015],	CAST(TRIM(REPLACE([2016], ',', '')) AS FLOAT) AS [2016]
INTO silver.ghg_investment_related
FROM bronze.ghg_investment_related

--
