/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*
===============================================================================
                         'gold.dim_customers'
===============================================================================
*/

-- ====================================================================
-- Checking after joining tables from Silver Layer
-- ====================================================================
-- Check for duplicates
-- Expectation: No results
SELECT cst_id, COUNT(*) FROM (
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
        ON ci.cst_key = la.cid
) AS t GROUP BY cst_id
HAVING COUNT(*) > 1

-- ==========================================================================
-- Checking differences between 'ci.cst_gndr' and 'ca.gen' for Data Integration
-- ==========================================================================
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid
ORDER BY 1,2

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check the View 'gold.dim_customers'
SELECT * FROM gold.dim_customers
SELECT DISTINCT gender FROM gold.dim_customers

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

/*
===============================================================================
                           'gold.dim_products'
===============================================================================
*/

-- ==========================================================================
-- Checking uniqueness
-- ==========================================================================
SELECT prd_key, COUNT(*) FROM (
    SELECT
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pn.prd_end_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM silver.crm_prd_info AS pn
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc
        ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL -- Filter out all historical data
) AS t GROUP BY prd_key
HAVING COUNT(*) > 1

-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================
-- Check the View 'gold.dim_products'
SELECT * FROM gold.dim_products

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

/*
===============================================================================
                           'gold.fact_sales'
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
SELECT * FROM gold.fact_sales

-- Check the data model connectivity between fact and dimensions
-- Foreign Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
