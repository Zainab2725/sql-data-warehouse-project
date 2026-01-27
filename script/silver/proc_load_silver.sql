/*
===============================================================================
Stored Procedure: silver.load_silver
===============================================================================
Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process 
    to transition data from the Bronze layer to the Silver layer. It cleans, 
    standardizes, and validates the raw data to ensure it is "ready for 
    analytics."

Action Performed:
    Executing this procedure will TRUNCATE all listed silver tables.
    Inserts transformed and cleansed data from Bronze into Silver tables

Execution:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
DECLARE @start_time DATETIME, @end_time DATETIME,@start_batch_time DATETIME,@end_batch_time DATETIME 
BEGIN 
	BEGIN TRY
	    SET @start_batch_time = GETDATE()
		PRINT '======================================================='
			PRINT 'Loading Silver Layer'
			PRINT '======================================================='

			PRINT '-------------------------------------------------------'
			PRINT 'Loading CRM'
			PRINT '-------------------------------------------------------'
		
	    SET @start_time = GETDATE()
		PRINT '>> Truncate existing data'
		TRUNCATE TABLE silver.crm_cust_info 
		PRINT '>> Inserting data'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_first_name,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_martial_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date 
		FROM
			(SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t WHERE flag_last = 1 

		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

		SET @start_time = GETDATE()
		PRINT '>> Truncate existing data'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting data'
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			-- Extract category part from prd_key (first 5 chars) and replace '-' with '_'
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			-- Extract product key part starting from 7th character
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			-- Replace NULL product cost with 0
			ISNULL(prd_cost, 0) AS prd_cost,
			-- Standardize product line codes into meaningful names
			CASE UPPER(TRIM(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			-- Convert start datetime to DATE
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			-- Set end date as one day before next start date
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
				AS DATE
			) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

		SET @start_time = GETDATE()
		PRINT '>> Truncate existing data'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting data'
		INSERT INTO silver.crm_sales_details (
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price
				)
				SELECT 
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					CASE 
						WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END AS sls_order_dt,
					CASE 
						WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END AS sls_ship_dt,
					CASE 
						WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END AS sls_due_dt,
					CASE 
						WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
							THEN sls_quantity * ABS(sls_price)
						ELSE sls_sales
					END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
					sls_quantity,
					CASE 
						WHEN sls_price IS NULL OR sls_price <= 0 
							THEN sls_sales / NULLIF(sls_quantity, 0)
						ELSE sls_price  -- Derive price if original value is invalid
					END AS sls_price
				FROM bronze.crm_sales_details;
		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')

		PRINT '-------------------------------------------------------'
		PRINT 'Loading ERP'
		PRINT '-------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncate existing data'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting data'
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) --Remove 'NAS' prefix if available
				 ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END bdate, -- set future date as null
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				 ELSE 'n/a' -- Normalize gender values and handle unknown cases
			END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

	  PRINT '>> Truncate existing data'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting data'
		INSERT INTO silver.erp_loc_a101 (cid,cntry)
		SELECT
		REPLACE(cid,'-','') cid,
		CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
			 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a' 
			 ELSE TRIM(cntry)
		END cntry -- normalizing and handling missing and blank country
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

		SET @start_time = GETDATE()
		PRINT '>> Truncate existing data'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting data'
		INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintainance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()

		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

		SET @end_batch_time = GETDATE()

		PRINT('>> Silver Load Duration:' + CAST(DATEDIFF(second,@start_batch_time,@end_batch_time) AS NVARCHAR) + 'seconds')
		PRINT('-------------------------------------------------------')

	END TRY
	BEGIN CATCH
	    PRINT '==================================================='
		PRINT 'ERROR OCCOURED DURING LOADING SILVER LAYER'
		PRINT 'ERROR : ' + ERROR.MESSAGE()
		PRINT 'ERROR : ' + CAST(ERROR.NUMBER() AS NVARCHAR)
		PRINT 'ERROR : ' + CAST(ERROR.STATE() AS NVARCHAR)
		PRINT '==================================================='
	END CATCH
END
