/*
===============================================================================
Product Report
===============================================================================

Purpose:
- This report consolidates key product metrics and behaviors.

Highlights:
1. Gathers essential fields such as product name, category, subcategory, and cost.
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
3. Aggregates product-level metrics:
   - total orders
   - total sales
   - total quantity sold
   - total customers (unique)
   - lifespan (in months)
4. Calculates valuable KPIs:
   - recency (months since last sale)
   - average order revenue (AOR)
   - average monthly revenue
*/
CREATE VIEW gold.report_products as
with base_query as (
/*--------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------*/
select
p.product_name,
p.category,
p.subcategory,
p.product_key,
p.cost,
f.order_date,
f.sales_amount,
f.quantity,
f.order_number,
f.customer_key
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null 
),
product_aggregration as (
/*--------------------------------------------------------------------
2) Product Aggregrations: Summarizes key metrics at the customer level
---------------------------------------------------------------------*/
select
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	DATEDIFF(month, MIN(order_date), MAX(Order_date)) as lifespan,
	max(order_date) as last_order_date,
	COUNT(DISTINCT order_number) as total_orders,
	COUNT(DISTINCT customer_key) as total_customers,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	ROUND(AVG(CAST(sales_amount as Float)/NULLIF(quantity, 0)),1) as avg_selling_price	
from base_query
group by product_key,product_name, category, subcategory,cost
)

select
product_name,
product_key,
category,
subcategory,
cost,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) as recent_date_order,
case 
	when total_sales > 1000000 then 'High Performer'
	when total_sales between 500000 and 1000000 then 'Mid Range'
	else 'Low Performers'
end as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
avg_selling_price,
--average order revenue (AOR)
case 
	when total_orders = 0 then 0
	else total_sales/total_orders
end as avg_order_revenue,
--average monthly revenue
case 
	when lifespan = 0 then total_sales
	else total_sales/lifespan
end as avg_monthly_revenue
from product_aggregration
--order by avg_monthly_revenue desc