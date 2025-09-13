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
  country NVARCHAR(MAX),	iso2 NVARCHAR(MAX),	iso3 NVARCHAR(MAX),	indicator_used NVARCHAR(MAX), unit NVARCHAR(MAX), 
  cts_code NVARCHAR(MAX),	cts_name NVARCHAR(MAX), cts_full_descriptor	NVARCHAR(MAX), industry NVARCHAR(MAX),	scale NVARCHAR(MAX),
  [1995] FLOAT(53),	[1996] FLOAT(53),	[1997] FLOAT(53),	[1998] FLOAT(53),	[1999] FLOAT(53),	[2000] FLOAT(53),	[2001] FLOAT(53),	[2002] FLOAT(53),	[2003] FLOAT(53),	[2004] FLOAT(53),	[2005] FLOAT(53),	[2006] FLOAT(53),
  [2007] FLOAT(53),	[2008] FLOAT(53),	[2009] FLOAT(53),	[2010] FLOAT(53),	[2011] FLOAT(53),	[2012] FLOAT(53),	[2013] FLOAT(53),	[2014] FLOAT(53),	[2015] FLOAT(53),	[2016] FLOAT(53),	[2017] FLOAT(53),	[2018] FLOAT(53),

	  dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

