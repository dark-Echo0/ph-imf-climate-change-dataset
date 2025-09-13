/*
=================================================================================
DDL Script: Create Bronze Tables
=================================================================================
Script Purpose:
    This script creates tables in the 'bronce' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
=================================================================================
*/

GO

IF OBJECT_ID('bronze.ghg_co2_emissions', 'U') IS NOT NUll
	DROP TABLE bronze.ghg_co2_emissions; 
GO
  
CREATE TABLE bronze.ghg_co2_emissions(
  country NVARCHAR(15),	iso2 NVARCHAR(10),	iso3 NVARCHAR(10),	indicator_used NVARCHAR(50), unit NVARCHAR(50), 
  cts_code NVARCHAR(15),	cts_name NVARCHAR(50), cts_full_descriptor	NVARCHAR(MAX), industry NVARCHAR(MAX),	scale NVARCHAR(10),
  [1995] FLOAT,	[1996] FLOAT,	[1997] FLOAT,	[1998] FLOAT,	[1999] FLOAT,	[2000] FLOAT,	[2001] FLOAT,	[2002] FLOAT,	[2003] FLOAT,	[2004] FLOAT,	[2005] FLOAT,	[2006] FLOAT,
  [2007] FLOAT,	[2008] FLOAT,	[2009] FLOAT,	[2010] FLOAT,	[2011] FLOAT,	[2012] FLOAT,	[2013] FLOAT,	[2014] FLOAT,	[2015] FLOAT,	[2016] FLOAT,	[2017] FLOAT,	[2018] FLOAT,

	  dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

