
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
Country NVARCHAR(20), ISO2 NVARCHAR(10), ISO3 NVARCHAR(10),
Indicator NVARCHAR(200),	Unit NVARCHAR(50), Source NVARCHAR(MAX),
CTS_Code NVARCHAR(20),	CTS_Name NVARCHAR(50), 	CTS_Full_Descriptor NVARCHAR(MAX),
Industry NVARCHAR(20),	Gas_Type NVARCHAR(20), 	Scale NVARCHAR(10), 	
[1970] FLOAT(53),	[1971] FLOAT(53), [1972] FLOAT(53), [1973] FLOAT(53), 
[1974] FLOAT(53),	[1975] FLOAT(53), [1976] FLOAT(53), [1977] FLOAT(53),	
[1978] FLOAT(53),	[1979] FLOAT(53), [1980] FLOAT(53), [1981] FLOAT(53),	
[1982] FLOAT(53),	[1983] FLOAT(53), [1984] FLOAT(53), [1985] FLOAT(53),	
[1986] FLOAT(53),	[1987] FLOAT(53), [1988] FLOAT(53), [1989] FLOAT(53),
[1990] FLOAT(53),	[1991] FLOAT(53), [1992] FLOAT(53), [1993] FLOAT(53),	
[1994] FLOAT(53),	[1995] FLOAT(53), [1996] FLOAT(53), [1997] FLOAT(53),
[1998] FLOAT(53),	[1999] FLOAT(53), [2000] FLOAT(53), [2001] FLOAT(53),
[2002] FLOAT(53),	[2003] FLOAT(53), [2004] FLOAT(53), [2005] FLOAT(53),	
[2006] FLOAT(53),	[2007] FLOAT(53), [2008] FLOAT(53), [2009] FLOAT(53),	
[2010] FLOAT(53),	[2011] FLOAT(53), [2012] FLOAT(53), [2013] FLOAT(53),	
[2014] FLOAT(53),	[2015] FLOAT(53), [2016] FLOAT(53), [2017] FLOAT(53),	
[2018] FLOAT(53),	[2019] FLOAT(53), [2020] FLOAT(53), [2021] FLOAT(53),
[2022] FLOAT(53),	[2023] FLOAT(53), current_dt DATETIME2 DEFAULT GETDATE()
);
GO
