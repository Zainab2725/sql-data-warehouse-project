/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	cus.cst_id AS customer_id,
	cus.cst_key AS customer_number,
	cus.cst_firstname AS first_name,
	cus.cst_lastname AS last_name,
	cl.cntry AS country,
	cus.cst_marital_status  AS marital_status,
	CASE WHEN cus.cst_gndr != 'n/a' THEN cus.cst_gndr
	ELSE COALESCE(cd.gen,'n/a')
	END gender,
	cd.bdate AS birthdate,
	cus.cst_create_date AS create_date
FROM silver.crm_cust_info cus
LEFT JOIN silver.erp_cust_az12 cd
ON cus.cst_key = cd.cid
LEFT JOIN silver.erp_loc_a101 cl
ON cus.cst_key = cl.cid
  
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
  
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pi.prd_cost AS product_cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL


-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cus.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date ,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cus
ON sd.sls_cust_id = cus.customer
