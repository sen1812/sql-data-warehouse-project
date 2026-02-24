-- Find the Total Sales
select SUM(sales_amount) as total_sales from gold.fact_sales
-- Find how many items are sold
select SUM(quantity) as total_quantity from gold.fact_sales 
-- Find the average selling price
select AVG(price) as average_price from gold.fact_sales
-- Find the Total number of Orders
select count(order_number) from gold.fact_sales
select COUNT(DISTINCT order_number) as distinct_orders from gold.fact_sales
-- Find the total number of products
select COUNT(product_name) as total_products from gold.dim_products
select COUNT(DISTINCT(product_name)) as distinct_products from gold.dim_products
-- Find the total number of customers
select COUNT(customer_key) as total_customers from gold.dim_customers
-- Find the total number of customers that has placed an order
select count(DISTINCT customer_key) as total_customers from gold.fact_sales

select 'Total Sales' as measure_name, SUM(sales_amount) as measure_value FROM gold.fact_sales
UNION ALL
select 'Total Quantity' as measure_name, SUM(quantity) from gold.fact_sales
UNION ALL
select 'Average Price', AVG(price) as average_price from gold.fact_sales
UNION ALL
select 'Total Nr. Orders', COUNT(DISTINCT order_number) from gold.fact_sales
UNION ALL
select 'Total Nr. Products', COUNT(DISTINCT(product_name)) from gold.dim_products
UNION ALL
select 'Total Nr. Customers', COUNT(customer_key) from gold.dim_customers