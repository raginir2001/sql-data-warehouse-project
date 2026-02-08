
/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
   - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.

*/

--====================================================================
--Checking 'silver.crm_cust_info'
--====================================================================
--Check for NULLS Duplicates in Primary Key
--Expectation: No Results
SELECT
   cst_id,
   COUNT(*)
FROM silver.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL;

--Check for Unwanted Spaces
--Expectation: No Results
SELECT
    cst key
FROM silver.crm_cust_info
WHERE cust key != TRIM(cust_key);

--Data Standardization & Consistency
SELECT DISTINCT
    cust_marital_status
FROM silver.crm_cust_info;

--=================================================================
--Checking 'silver.crm_prd_info"
--=================================================================
--Check for NULLS or  Duplicates in Primary Key
--Expectation: No Results
SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info;
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--Check for Unwanted Spaces
--Expectation: No Results
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Check for NULLS or Negative Values in Cost
--Results Expectation: No Results
SELECT
    prd cost
FROM silver.crm_prd_info
WHERE pro_cost < 0 OR prd_cost IS NULL;

--Data Standardization & Consistency
SELECT DISTINCT
    prd line
FROM silver.crm_prd_info;

--Check for Invalid Date Orders (Start Date End Date)
--Expectation: No Results
SELECT
*
FROM silver.crm_prd_info
WHERE prd end_dt <  prd start_dt;

--=========================================================================
--Checking 'silver.crm_sales details
--=========================================================================
--Check for Invalid Dates
--Expectation: No Invalid Dates
SELECT
    NULLIF(sis_due dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt = 0
    OR LEN(Sls_due_dt) != 8
    OR sls_due_dt > 20500101
    OR sls_due_dt < 19000101;

--Check for Invalid Date Orders (Order Date Shipping/Due Dates)
--Expectation: No Results
SELECT
    *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

--Check Data Consistency: Sales Quantity * Price
--Expectation: No Results
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--======================================================================
--checking 'silver.erp cust az12'
--======================================================================
--Identify Out-of-Range Dates
--Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();

--Data Standardization & Consistency
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12;

--=========================================================================
--Checking 'silver.erp_loc_a101"
--=========================================================================
--Data Standardization & Consistency
SELECT DISTINCT
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

--============================================================
--Checking 'silver.erp_pa_cat giv2'
--============================================================
--Check for Unwanted Spaces.
--Expectation: No Results
SELECT
     *
FROM silver_erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintanence)

-- Data Standardization & Consistency
SELECT DISTINCT
    maintanence 
FROM silver.erp_px_cat_g1v2;
