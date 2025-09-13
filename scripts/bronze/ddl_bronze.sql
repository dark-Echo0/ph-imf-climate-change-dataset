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
  [1995] NVARCHAR(MAX),	[1996] NVARCHAR(MAX),	[1997] NVARCHAR(MAX),	[1998] NVARCHAR(MAX),	[1999] NVARCHAR(MAX),	[2000] NVARCHAR(MAX),	[2001] NVARCHAR(MAX),	[2002] NVARCHAR(MAX),	[2003] NVARCHAR(MAX),	[2004] NVARCHAR(MAX),	[2005] NVARCHAR(MAX),	[2006] NVARCHAR(MAX),
  [2007] NVARCHAR(MAX),	[2008] NVARCHAR(MAX),	[2009] NVARCHAR(MAX),	[2010] NVARCHAR(MAX),	[2011] NVARCHAR(MAX),	[2012] NVARCHAR(MAX),	[2013] NVARCHAR(MAX),	[2014] NVARCHAR(MAX),	[2015] NVARCHAR(MAX),	[2016] NVARCHAR(MAX),	[2017] NVARCHAR(MAX),	[2018] NVARCHAR(MAX),

	  dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

