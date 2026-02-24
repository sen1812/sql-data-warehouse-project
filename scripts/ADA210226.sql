/*Segment products into cost ranges and count how many products fall into each segment*/

with product_segment as (
select 
product_key,
product_name,
cost,
case
	when cost < 100 then 'Below 100'
	when cost between 100 and 500 then '100-500'
	when cost between 500 and 1000 then '500-1000'
	else 'above 1000'
end cost_range
from gold.dim_products)

select
cost_range,
COUNT(product_key) as total_products
from product_segment
GROUP by cost_range
order by total_products DESC


/*Group customers into three segments based on their spending behavior:
- VIP: Customers with at least 12 months of history and spending more than €5,000.
- Regular: Customers with at least 12 months of history but spending €5,000 or less.
- New: Customers with a lifespan less than 12 months.

And find the total number of customers by each group
*/
WITH customer_spending as (

select
gc.customer_key,
SUM(gs.sales_amount) as total_spending,
MIN(order_date) as first_order,
MAX(order_date) as last_order,
DATEDIFF(month, MIN(order_date), MAX (order_date)) as lifespan
from gold.fact_sales gs
left join gold.dim_customers gc
on gs.customer_key = gc.customer_key
group by gc.customer_key)

select
customer_segment,
count(customer_key) as total_customers
from(select
	customer_key,
	total_spending,
	lifespan,
	case 
		when lifespan >= 12 and total_spending > 5000 then 'VIP'
		when lifespan >= 12 and total_spending <= 5000 then 'Regular'
		else 'New'
	end customer_segment
	from customer_spending
	) t
group by customer_segment
order by total_customers desc