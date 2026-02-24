-- Check for nulls or duplicates in primary key
-- Expectation: No Result
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER (TRIM (cst_marital_status)) = 'S' then 'Single'
	 WHEN UPPER (TRIM (cst_marital_status)) = 'M' then 'Married'
	 ELSE 'unknown'
END cst_marital_status,
CASE WHEN UPPER (TRIM (cst_gndr)) = 'F' then 'Female'
	 WHEN UPPER (TRIM (cst_gndr)) = 'M' then 'Male'
	 ELSE 'unknown'
END cst_gndr,
cst_create_date
from 
	(	select 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as last_flag
		from bronze.crm_cust_info
	) t where last_flag = 1;

-- check for unwanted spaces
-- Expectation: No Results
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

--

select 
distinct cst_marital_status
from silver.crm_cust_info

select * from silver.crm_cust_info