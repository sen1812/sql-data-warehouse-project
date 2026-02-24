INSERT INTO silver.crm_sales_details
(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id, 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)		
		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 or LEN (sls_order_dt) != 8 THEN NULL
			ELSE CAST (CAST(sls_order_dt as varchar) as DATE)
		END as sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 or LEN (sls_ship_dt) != 8 THEN NULL
			ELSE CAST (CAST(sls_ship_dt as varchar) as DATE)
		END as sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 or LEN (sls_due_dt) != 8 THEN NULL
			ELSE CAST (CAST(sls_due_dt as varchar) as DATE)
		END as sls_due_dt,
		CASE 
			WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		CASE
			when sls_price is null or sls_price <= 0
				then sls_sales/NULLIF(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_details;

select * from silver.crm_sales_details;

