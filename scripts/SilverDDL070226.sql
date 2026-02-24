--Check for NULLs or duplicates in primary key
--Expectations: No Result

select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
HAVING count(*) > 1 OR prd_id is NULL;

select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is NULL;

select distinct prd_line
from  silver.crm_prd_info;

select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;


select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

IF OBJECT_ID('silver.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id        INT,
	cat_id		  NVARCHAR(50),
    prd_key       NVARCHAR(50),
    prd_nm        NVARCHAR(50),
    prd_cost      INT,
    prd_line      NVARCHAR(50),
    prd_start_dt  DATE,
    prd_end_dt    DATE,
	dwh_create_date  	DATETIME2 DEFAULT GETDATE()
);

INSERT into silver.crm_prd_info
(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
	select
		prd_id,
		REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- EXTRACT Category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,			-- EXTRACT Product Key
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER (TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'Unknown'
		END as prd_line,
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 as prd_end_dt
		from bronze.crm_prd_info;
select * from silver.crm_prd_info

