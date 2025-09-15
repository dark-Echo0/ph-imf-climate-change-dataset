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
  [1995],	[1996],	[1997],	[1998],	[1999],	[2000],	[2001],	[2002],	[2003],	[2004],	[2005],	[2006],
  [2007],	[2008],	[2009],	[2010],	[2011],	[2012],	[2013],	[2014],	[2015],	[2016],	[2017],	[2018]
INTO silver.ghg_co2_emissions
FROM bronze.ghg_co2_emissions
GO

-- Create 'silver.ghg_carbon_footprints' table
IF OBJECT_ID('silver.ghg_carbon_footprints', 'U') IS NOT NUll
	DROP TABLE silver.ghg_carbon_footprints; 

SELECT
  country,	iso2,	iso3,	indicator_used, 
  unit, cts_code,
  [1995],	[1996],	[1997],	[1998],	[1999],	[2000],	[2001],	[2002],	[2003],	[2004],	[2005],	[2006],
  [2007],	[2008],	[2009],	[2010],	[2011],	[2012],	[2013],	[2014],	[2015],	[2016],	[2017],	[2018],
  [2019], [2020],   [2021]
INTO silver.ghg_carbon_footprints
FROM bronze.ghg_carbon_footprints

-- Create 'silver.ghg_investment_related' table
IF OBJECT_ID('silver.ghg_investment_related', 'U') IS NOT NUll
	DROP TABLE silver.ghg_investment_related; 
  
SELECT
  country,	iso2,	iso3,	indicator_used, 
  unit, cts_code, sector,	
  [2005],	[2006], [2007],	[2008],	[2009], [2010],	
  [2011],	[2012],	[2013],	[2014],	[2015],	[2016] 
INTO silver.ghg_investment_related
FROM bronze.ghg_investment_related

--
