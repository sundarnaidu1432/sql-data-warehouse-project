CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @Proc_Start_Time DATETIME = GETDATE();
	DECLARE @Start_Time DATETIME, @End_Time DATETIME;

	BEGIN TRY
			PRINT '============================================';
			PRINT 'Loading Bronze by Truncating and Inserting';
			PRINT '============================================';

			-- CRM TABLES
			PRINT '--------------------------------------------';
			PRINT 'CRM TABLES';
			PRINT '--------------------------------------------';

			--silver.crm_cust_info
			SET @Start_Time = GETDATE()
			PRINT'Truncating silver.crm_cust_info table';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT'Inserting silver.crm_cust_info table';
			INSERT INTO silver.crm_cust_info(
				cst_id,cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				 WHEN 'M' THEN 'Married'
				 WHEN 'S' THEN 'Single'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE UPPER(TRIM(cst_gndr))
				 WHEN 'M' THEN 'Male'
				 WHEN 'F' THEN 'Female'
				 ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
			FROM(
			SELECT*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag --last creation date 
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL)t
			WHERE flag = 1
			SET @End_Time = GETDATE()
			PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

			--silver.crm_prd_info
			SET @Start_Time = GETDATE()
			PRINT'Truncating silver.crm_prd_info table';
			TRUNCATE TABLE silver.crm_prd_info;
			PRINT'Inserting silver.crm_prd_info table';
			INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,--Extracted category id
			SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,     --Extracted product key
			prd_nm,
			ISNULL(prd_cost,0) prd_cost,--Replaced nulls with zero
			CASE UPPER(TRIM(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'M' THEN 'Mountain'
				WHEN 'T' THEN 'Touring'
				WHEN 'S' THEN 'Other Sales'
				ELSE 'n/a'
				END AS prd_line,
			CAST(prd_start_dt AS DATE) prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS  prd_end_dt -- product end date based on next producte start date
			FROM bronze.crm_prd_info
			SET @End_Time = GETDATE()
			PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

			--silver.crm_sales_details
			SET @Start_Time = GETDATE()
			PRINT'Truncating silver.crm_sales_details table';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT'Inserting silver.crm_sales_details table';
			INSERT INTO silver.crm_sales_details(
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
			SELECT
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
					ELSE CAST(CAST(sls_order_dt AS NVARCHAR)AS DATE)
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
					ELSE CAST(CAST(sls_ship_dt AS NVARCHAR)AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
					ELSE CAST(CAST(sls_due_dt AS NVARCHAR)AS DATE)
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != (ABS(sls_price)*sls_quantity) THEN (ABS(sls_price)*sls_quantity)
					ELSE sls_sales
				END AS sls_sales,
				sls_quantity,
				CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0)
					 ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details
			SET @End_Time = GETDATE()
			PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

			  -- ERP TABLES
			PRINT '--------------------------------------------';
			PRINT 'ERP TABLES';
			PRINT '--------------------------------------------';

			--silver.erp_cust_az12
			SET @Start_Time = GETDATE()
			PRINT'Truncating silver.erp_cust_az12 table';
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT'Inserting silver.erp_cust_az12 table';
			INSERT INTO silver.erp_cust_az12(
				 cid,
				 bdate,
				 gen
			)
			SELECT
				CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					 else cid
				END cid,
				CASE 
					WHEN bdate >GETDATE() THEN NULL
					ELSE bdate
				END bdate,
				CASE 
					WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					ELSE 'n/a'
				END gen
			FROM bronze.erp_cust_az12
			SET @End_Time = GETDATE()
			PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

			--silver.erp_loc_a101
			SET @Start_Time = GETDATE()
			PRINT'Truncating silver.erp_loc_a101 table';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT'Inserting silver.erp_loc_a101 table';
			INSERT INTO silver.erp_loc_a101(
				cid,
				cntry)
			SELECT
			REPLACE(cid,'-','')cid,
			CASE
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
				WHEN cntry = ' ' OR cntry IS NULL THEN 'n/a'
				else cntry
				end AS cntry
			FROM bronze.erp_loc_a101
			SET @End_Time = GETDATE()
			PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

			--silver.erp_px_cat_g1v2
			SET @Start_Time = GETDATE()
			PRINT'Truncating silver.erp_px_cat_g1v2 table';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT'Inserting silver.erp_px_cat_g1v2 table';
			INSERT INTO silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
			)
			SELECT
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2
			SET @End_Time = GETDATE()
			PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

			PRINT '============================================';
			PRINT 'Total Load Time: ' + CAST(DATEDIFF(SECOND,@Proc_Start_Time,GETDATE()) AS NVARCHAR) + ' Seconds';
			PRINT '============================================';
		END TRY
		BEGIN CATCH
			PRINT 'ERROR Message: ' + ERROR_MESSAGE();
			PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() AS VARCHAR);
			PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		END CATCH
END
