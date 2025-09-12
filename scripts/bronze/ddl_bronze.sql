
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
--Create Table for National Greenhouse Gas Emissions Inventories by the sector of origin and National Mitigation Targets as determined by the countries.
IF OBJECT_ID('bronze.nat_inventory_target', 'U') IS NOT NUll
	DROP TABLE bronze.nat_inventory_target; 
GO
  
CREATE TABLE bronze.nat_inventory_target(
Country NVARCHAR(15), ISO2 NVARCHAR(10), ISO3 NVARCHAR(10),
Indicator NVARCHAR(50),	Unit NVARCHAR(50), Source NVARCHAR(MAX),
CTS_Code NVARCHAR(20),	CTS_Name NVARCHAR(MAX), 	CTS_Full_Descriptor NVARCHAR(MAX),
Industry NVARCHAR(50),	Gas_Type VARCHAR(50), 	Scale VARCHAR(10), 	
[1970] FLOAT,	[1971] FLOAT, [1972] FLOAT, [1973] FLOAT, 
[1974] FLOAT,	[1975] FLOAT, [1976] FLOAT, [1977] FLOAT,	
[1978] FLOAT,	[1979] FLOAT, [1980] FLOAT, [1981] FLOAT,	
[1982] FLOAT,	[1983] FLOAT, [1984] FLOAT, [1985] FLOAT,	
[1986] FLOAT,	[1987] FLOAT, [1988] FLOAT, [1989] FLOAT,
[1990] FLOAT,	[1991] FLOAT, [1992] FLOAT, [1993] FLOAT,	
[1994] FLOAT,	[1995] FLOAT, [1996] FLOAT, [1997] FLOAT,
[1998] FLOAT,	[1999] FLOAT, [2000] FLOAT, [2001] FLOAT,
[2002] FLOAT,	[2003] FLOAT, [2004] FLOAT, [2005] FLOAT,	
[2006] FLOAT,	[2007] FLOAT, [2008] FLOAT, [2009] FLOAT,	
[2010] FLOAT,	[2011] FLOAT, [2012] FLOAT, [2013] FLOAT,	
[2014] FLOAT,	[2015] FLOAT, [2016] FLOAT, [2017] FLOAT,	
[2018] FLOAT,	[2019] FLOAT, [2020] FLOAT, [2021] FLOAT,
[2022] FLOAT,	[2023] FLOAT, current_dt DATETIME2 DEFAULT GETDATE()
);
GO
