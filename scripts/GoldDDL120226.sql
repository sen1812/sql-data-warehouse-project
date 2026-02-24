
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
