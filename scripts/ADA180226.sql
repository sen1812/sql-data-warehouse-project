select 
Year(order_date) as Order_year,
SUM(sales_amount) as total_sales,
count(customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by Year(order_date)
order by Year(order_date)


select 
DATETRUNC(month, order_date) as Order_date,
SUM(sales_amount) as total_sales,
count(customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month, order_date)
order by DATETRUNC(month, order_date)


-- Calculate the total sales per month
-- and the running total of sales over time
select
order_date,
total_sales,
SUM(total_sales) over ( ORDER by order_date) as running_total_sales
from
(
		select
		DATETRUNC(month,order_date) as order_date,
		sum(sales_amount) as total_sales
		from gold.fact_sales
		where order_date is not null
		group by DATETRUNC(month,order_date)
)t;

select
order_date,
total_sales,
SUM(total_sales) over ( ORDER by order_date) as running_total_sales,
AVG(average_price) over (ORDER by order_date) as moving_average_sales
from
(
		select
		DATETRUNC(year,order_date) as order_date,
		sum(sales_amount) as total_sales,
		AVG(price) as average_price
		from gold.fact_sales
		where order_date is not null
		group by DATETRUNC(year,order_date)
)t;