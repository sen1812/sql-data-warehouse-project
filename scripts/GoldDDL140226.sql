	
	select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_create_date,
	ca.bdate,
	CASE
		WHEN ci.cst_gndr != 'Unknown' then ci.cst_gndr -- CRM is the master for gender info
		else COALESCE (ca.gen, 'Unknown')
	END as new_gen,
	la.cntry
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid
	
	CREATE VIEW gold.dim_customers AS
	select 
			ROW_NUMBER() over (ORDER BY cst_id) as customer_key,
			ci.cst_id AS customer_id,
			ci.cst_key AS customer_number,
			ci.cst_firstname as first_name,
			ci.cst_lastname as last_name,
			la.cntry as country,
			ci.cst_marital_status as marital_status,
				CASE
				WHEN ci.cst_gndr != 'Unknown' then ci.cst_gndr -- CRM is the master for gender info
				else COALESCE (ca.gen, 'Unknown')
			END as gender,
			ca.bdate as birthdate,
			ci.cst_create_date as create_date	
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid
	where ci.cst_id is not null;


	CREATE VIEW gold.dim_products as
		select 
			ROW_NUMBER () OVER (ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
			pn.prd_id as product_id,
			pn.prd_key as product_number,
			pn.prd_nm as product_name,
			pn.cat_id as category_id,
			pc.cat as category,
			pc.subcat as subcategory,
			pc.maintenance,
			pn.prd_cost as cost,
			pn.prd_line as product_line,
			pn.prd_start_dt as start_date
		from silver.crm_prd_info pn
		left join silver.erp_px_cat_gv1v2 pc
		on pn.cat_id = pc.id
		where prd_end_dt is null --filter out all historical data


CREATE VIEW gold.fact_sales as
	select 
		sd.sls_ord_num as order_number,
		pr.product_key,
		cu.customer_key,
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt as shipping_date,
		sd.sls_due_dt as due_date,
		sd.sls_sales as sales_amount,
		sd.sls_quantity as quantity,
		sd.sls_price as price
	from silver.crm_sales_details sd
	left join gold.dim_products pr
	on sd.sls_prd_key = pr.product_number
	left join gold.dim_customers cu
	on sd.sls_cust_id = cu.customer_id

