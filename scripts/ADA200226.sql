/* Analyse the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous years sale */

with yearly_product_sales as (
		select
		YEAR(f.order_date) as order_year,
		p.product_name,
		sum(f.sales_amount) as current_year_sales
		from gold.fact_sales f
		left join gold.dim_products P
		on f.product_key = p.product_key
		where f.order_date is not null
		group by YEAR(f.order_date),p.product_name
) 

select
order_year,
product_name,
current_year_sales,
AVG(current_year_sales) over (Partition by product_name) avg_sales,
current_year_sales - AVG(current_year_sales) over (Partition by product_name) as diff_avg,
case 
	when current_year_sales - AVG(current_year_sales) over (Partition by product_name) > 0 then 'Above Avg'
	when current_year_sales - AVG(current_year_sales) over (Partition by product_name) < 0 then 'Below Avg'
	Else 'Avg'
end avg_status,
lag(current_year_sales) over (partition by product_name order by order_year) py_sales,
current_year_sales - lag(current_year_sales) over (partition by product_name order by order_year) as diff_py,
Case
	when current_year_sales - lag(current_year_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
	when current_year_sales - lag(current_year_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	else 'No Change'
end py_change
from yearly_product_sales
order by product_name, order_year;

--Which categories contribute the most to overall sales

WITH category_sales as (
select
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)

select 
category,
total_sales,
SUM(total_sales) over () overall_sales,
CONCAT(Round((Cast (total_sales as Float)/SUM(total_sales) over ())* 100,2),'%') as percentage_of_total
from category_sales
order by total_sales desc