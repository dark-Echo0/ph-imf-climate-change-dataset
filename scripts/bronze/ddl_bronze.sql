
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
[1970] DECIMAL(20,15),	[1971] DECIMAL(20,15), [1972] DECIMAL(20,15), [1973] DECIMAL(20,15), 
[1974] DECIMAL(20,15),	[1975] DECIMAL(20,15), [1976] DECIMAL(20,15), [1977] DECIMAL(20,15),	
[1978] DECIMAL(20,15),	[1979] DECIMAL(20,15), [1980] DECIMAL(20,15), [1981] DECIMAL(20,15),	
[1982] DECIMAL(20,15),	[1983] DECIMAL(20,15), [1984] DECIMAL(20,15), [1985] DECIMAL(20,15),	
[1986] DECIMAL(20,15),	[1987] DECIMAL(20,15), [1988] DECIMAL(20,15), [1989] DECIMAL(20,15),
[1990] DECIMAL(20,15),	[1991] DECIMAL(20,15), [1992] DECIMAL(20,15), [1993] DECIMAL(20,15),	
[1994] DECIMAL(20,15),	[1995] DECIMAL(20,15), [1996] DECIMAL(20,15), [1997] DECIMAL(20,15),
[1998] DECIMAL(20,15),	[1999] DECIMAL(20,15), [2000] DECIMAL(20,15), [2001] DECIMAL(20,15),
[2002] DECIMAL(20,15),	[2003] DECIMAL(20,15), [2004] DECIMAL(20,15), [2005] DECIMAL(20,15),	
[2006] DECIMAL(20,15),	[2007] DECIMAL(20,15), [2008] DECIMAL(20,15), [2009] DECIMAL(20,15),	
[2010] DECIMAL(20,15),	[2011] DECIMAL(20,15), [2012] DECIMAL(20,15), [2013] DECIMAL(20,15),	
[2014] DECIMAL(20,15),	[2015] DECIMAL(20,15), [2016] DECIMAL(20,15), [2017] DECIMAL(20,15),	
[2018] DECIMAL(20,15),	[2019] DECIMAL(20,15), [2020] DECIMAL(20,15), [2021] DECIMAL(20,15),
[2022] DECIMAL(20,15),	[2023] DECIMAL(20,15), current_dt DATETIME2 DEFAULT GETDATE()
);
GO
