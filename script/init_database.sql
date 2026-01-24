/*
=============================================================================================
Create Database and Schemas
=============================================================================================
Purpose:
   This script sets up the 'DataWareHouse' database with a structured schema 
   following the Medallion Architecture. It performs the following steps:
      1. Checks if 'DataWareHouse' exists and drops it safely (forces single-user mode 
         and rolls back any active transactions).
      2. Creates a new 'DataWareHouse' database.
      3. Creates three schemas to organize data layers  
WARNING:
   Executing this script will permanently delete the existing 'DataWareHouse' database
   and all its data. Ensure you have a backup if needed before running this script.
*/

-- Create Database 'DataWarehouse'

USE master;
GO

--Dropping if Database exists 

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO

--Creating Database

CREATE DATABASE DataWareHouse
GO 

USE DataWareHouse;
GO

--Creating Schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

