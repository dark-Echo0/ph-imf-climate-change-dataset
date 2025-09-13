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
	DROP TABLE bronze.ghg_co2_emissions; 
GO
  
CREATE TABLE silver.ghg_co2_emissions(
  unit NVARCHAR(MAX), cts_code NVARCHAR(MAX),	cts_name NVARCHAR(MAX), industry NVARCHAR(MAX),
  [2000] FLOAT,	[2001] FLOAT,	[2002] FLOAT,	[2003] FLOAT,	[2004] FLOAT,	[2005] FLOAT,	[2006] FLOAT,
  [2007] FLOAT,	[2008] FLOAT,	[2009] FLOAT,	[2010] FLOAT,	[2011] FLOAT,	[2012] FLOAT,	[2013] FLOAT,	
  [2014] FLOAT,	[2015] FLOAT,	[2016] FLOAT,	[2017] FLOAT,	[2018] FLOAT,
  dwh_create_date DATETIME NOT NULL DEFAULT GETDATE();
);
GO
