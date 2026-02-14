## 4. Gold Layer Quality Checks

### Overview

The following quality checks are designed to validate the **integrity, consistency, and accuracy** of the Gold Layer data. These checks ensure:

- Uniqueness of surrogate keys in dimension tables
- Referential integrity between fact and dimension tables
- Validation of relationships in the data model for analytical purposes

### Usage Notes

- Run these checks after data loading into the Silver Layer
- Investigate and resolve any discrepancies found during the checks

---

### 4.1 Uniqueness Check – `gold.dim_customers`

**Expectation:** No results returned

```sql
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
``

-- ============================================================
-- Checking 'gold.product_key'
-- ============================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results

SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ============================================================
-- Checking 'gold.fact_sales'
-- ============================================================
-- Check the data model connectivity between fact and dimensions

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE
    p.product_key IS NULL
    OR c.customer_key IS NULL;
