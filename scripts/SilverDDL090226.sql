INSERT INTO silver.erp_cust_az12
(cid,
bdate,
gen)

		select 
		CASE
			WHEN cid like 'NAS%' then SUBSTRING(cid, 4,len(CID))
			else cid
		END cid,
		CASE
			WHEN bdate > GETDATE () THEN NULL
			ELSE bdate
		END bdate,	
		CASE
			WHEN UPPER (TRIM (gen)) in ('M', 'MALE') then 'Male'
			WHEN UPPER (TRIM (gen)) in ('F', 'Female') then 'Female'
			ELSE 'Unknown'
		end gen
		from bronze.erp_cust_az12
