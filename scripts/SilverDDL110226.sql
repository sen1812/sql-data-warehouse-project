CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Silver Layer';
		PRINT '=========================================';

	    PRINT '=========================================';
		PRINT 'Loading CRM Tables';
		PRINT '=========================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting Data into:silver.crm_cust_info';
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
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DateDiff (second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data into:silver.crm_prd_info';

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
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DateDiff (second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into:silver.crm_sales_details';

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
				from bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST (Datediff (second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		PRINT '--------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into:silver.erp_cust_az12';
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

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST (Datediff (second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into:silver.erp_loc_a101';
		Insert into silver.erp_loc_a101
		(
		cid,
		cntry)		
				select
				REPLACE(cid,'-','') as cid,
				CASE
					WHEN (TRIM (cntry)) = 'DE' THEN 'Germany'
					WHEN (TRIM (cntry)) in ('USA', 'US') THEN 'United States'
					WHEN (TRIM (cntry)) = '' or cntry IS NULL THEN 'n/a'
					else (TRIM (cntry))
				end as cntry	
				from bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST (Datediff (second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_gv1v2';
		TRUNCATE TABLE silver.erp_px_cat_gv1v2;
		PRINT '>> Inserting Data into:silver.erp_px_cat_gv1v2';
		insert into silver.erp_px_cat_gv1v2
		(
		id,
		cat,
		subcat,
		maintenance)
				select
				id,
				cat,
				subcat,
				maintenance
				from bronze.erp_px_cat_gv1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST (Datediff (second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @batch_end_time = GETDATE();
		PRINT '>> Loading Silver Layer is completed';
		PRINT ' - Total Load Duration: ' + CAST (Datediff (second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		END TRY
	    BEGIN CATCH
		PRINT '==============================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + Error_Message();
		PRINT 'Error Message' + CAST (Error_Number() AS NVARCHAR);
		PRINT 'Error Message' + CAST (Error_State() AS NVARCHAR);
		PRINT '=============================='
	    END CATCH
END

EXEC silver.load_silver