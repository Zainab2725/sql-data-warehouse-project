/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Purpose:
    This stored procedure loads raw source data into the Bronze layer of the
    Data Warehouse. It truncates existing bronze tables and performs bulk
    inserts from CSV source files to ensure a fresh and consistent reload.

Scope:
    - Loads CRM source data:
        • crm_cust_info
        • crm_prd_info
        • crm_sales_details
    - Loads ERP source data:
        • erp_cust_az12
        • erp_loc_a101
        • erp_px_cat_g1v2

Key Features:
    - Uses BULK INSERT for high-performance ingestion
    - Captures load duration for each table and overall batch
    - Implements TRY...CATCH error handling for controlled failure reporting

Warning:
    Executing this procedure will TRUNCATE all listed bronze tables.
    Ensure source files are available and validated before execution.

Execution:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME,@start_batch_time DATETIME,@end_batch_time DATETIME 
	BEGIN TRY
		
		PRINT '======================================================='
		PRINT 'Loading Bronze Layer'
		PRINT '======================================================='

		PRINT '-------------------------------------------------------'
		PRINT 'Loading CRM'
		PRINT '-------------------------------------------------------'

		SET @start_batch_time = GETDATE()
		SET @start_time = GETDATE()
		-- Truncate and loading crm_cust_info table
		PRINT '>> Truncating: crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Bulk Insert : crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

		-- Truncate and loading crm_prd_info table
		SET @start_time = GETDATE()
		PRINT '>> Truncating: crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Bulk Insert : crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

		-- Truncate and loading crm_sales_details table
		SET @start_time = GETDATE()
		PRINT '>> Truncating: crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Bulk Insert : crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')


		PRINT '-------------------------------------------------------'
		PRINT 'Loading crm'
		PRINT '-------------------------------------------------------'

		-- Truncate and loading erp_cust_az12 table
		SET @start_time = GETDATE()
		PRINT '>> Truncating: erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Bulk Insert : erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\sql\dwh_project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

	
		-- Truncate and loading erp_loc_a101 table
		SET @start_time = GETDATE()
		PRINT '>> Truncating: erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101
	
		PRINT '>> Bulk Insert : erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\sql\dwh_project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')


		-- Truncate and loading erp_px_cat_g1v2 table
		SET @start_time = GETDATE()
		PRINT '>> Truncating: erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
	
		PRINT '>> Bulk Insert : erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\sql\dwh_project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		); 

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')
		SET @end_batch_time = GETDATE();

		PRINT('==========================================================')
		PRINT('Bronze Total Load Duration')
		PRINT('>> Load Time: ' + CAST(DATEDIFF(second,@start_batch_time,@end_batch_time) AS NVARCHAR) + 'seconds')
		PRINT('==========================================================')
	END TRY
	BEGIN CATCH
		PRINT '==================================================='
		PRINT 'ERROR OCCOURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR : ' + ERROR.MESSAGE()
		PRINT 'ERROR : ' + CAST(ERROR.NUMBER() AS NVARCHAR)
		PRINT 'ERROR : ' + CAST(ERROR.STATE() AS NVARCHAR)
		PRINT '==================================================='

	END CATCH
END
