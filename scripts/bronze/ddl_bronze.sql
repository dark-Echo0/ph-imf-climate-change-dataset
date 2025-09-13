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
  [1995] FLOAT NULL,	[1996] FLOAT NULL,	[1997] FLOAT NULL,	[1998] FLOAT NULL,	[1999] FLOAT NULL,	[2000] FLOAT NULL,	[2001] FLOAT NULL,	[2002] FLOAT NULL,	[2003] FLOAT NULL,	[2004] FLOAT NULL,	[2005] FLOAT NULL,	[2006] FLOAT NULL,
  [2007] FLOAT NULL,	[2008] FLOAT NULL,	[2009] FLOAT NULL,	[2010] FLOAT NULL,	[2011] FLOAT NULL,	[2012] FLOAT NULL,	[2013] FLOAT NULL,	[2014] FLOAT NULL,	[2015] FLOAT NULL,	[2016] FLOAT NULL,	[2017] FLOAT NULL,	[2018] FLOAT NULL,

	  dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

