SELECT TOP (1000) [order_number]
      ,[product_key]
      ,[customer_key]
      ,[order_date]
      ,[shipping_date]
      ,[due_date]
      ,[sales_amount]
      ,[quantity]
      ,[price]
  FROM [DataWarehouseAnalytics].[gold].[fact_sales]

  select * from INFORMATION_SCHEMA.TABLES

  select DISTINCT country from gold.dim_customers

  select DISTINCT category, subcategory, product_name from gold.dim_products

  select 
  MIN(order_date) first_order_date,
  MAX(order_date) last_order_date,
  DATEDIFF(year, MIN(order_date), MAX(order_date)) as order_range_years
  from gold.fact_sales

  select
  MIN(birthdate) oldest_customer,
  DATEDIFF(year,MIN(birthdate), GETDATE()) as oldest_age,
  MAX(birthdate) youngest_customer,
  DATEDIFF(year,MAX(birthdate), GETDATE()) as youngest_age,
  DATEDIFF(year, MIN(birthdate), MAX(birthdate)) as Age_difference
  from gold.dim_customers
