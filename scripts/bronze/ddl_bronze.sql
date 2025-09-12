
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
1970 DECIMAL(10,20),	1971 DECIMAL(10,20), 1972 DECIMAL(10,20), 1973 DECIMAL(10,20), 
1974 DECIMAL(10,20),	1975 DECIMAL(10,20), 1976 DECIMAL(10,20),	1977 DECIMAL(10,20),	
1978 DECIMAL(10,20),	1979 DECIMAL(10,20), 1980 DECIMAL(10,20),	1981 DECIMAL(10,20),	
1982 DECIMAL(10,20),	1983 DECIMAL(10,20), 1984 DECIMAL(10,20), 1985 DECIMAL(10,20),	
1986 DECIMAL(10,20),	1987 DECIMAL(10,20), 1988	DECIMAL(10,20), 1989 DECIMAL(10,20),
1990 DECIMAL(10,20),	1991 DECIMAL(10,20), 1992 DECIMAL(10,20),	1993 DECIMAL(10,20),	
1994 DECIMAL(10,20),	1995 DECIMAL(10,20), 1996	DECIMAL(10,20), 1997 DECIMAL(10,20),
1998 DECIMAL(10,20),	1999 DECIMAL(10,20), 2000 DECIMAL(10,20),	2001 DECIMAL(10,20),
2002 DECIMAL(10,20),	2003 DECIMAL(10,20), 2004 DECIMAL(10,20),	2005 DECIMAL(10,20),	
2006 DECIMAL(10,20),	2007 DECIMAL(10,20), 2008 DECIMAL(10,20),	2009 DECIMAL(10,20),	
2010 DECIMAL(10,20),	2011 DECIMAL(10,20), 2012 DECIMAL(10,20),	2013 DECIMAL(10,20),	
2014 DECIMAL(10,20),	2015 DECIMAL(10,20), 2016 DECIMAL(10,20),	2017 DECIMAL(10,20),	
2018 DECIMAL(10,20),	2019 DECIMAL(10,20), 2020 DECIMAL(10,20),	2021 DECIMAL(10,20)	
2022 DECIMAL(10,20),	2023 DECIMAL(10,20), current_date DATETIME2 DEFAULT GETDATE()
);
GO

