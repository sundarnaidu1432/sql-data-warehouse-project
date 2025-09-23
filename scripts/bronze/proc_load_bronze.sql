/*
===================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load bronze;
======================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
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

        -- crm_cust_info
        SET @Start_Time = GETDATE();
        PRINT 'bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\kandr\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
  			FIRSTROW = 2, 
  			FIELDTERMINATOR = ',', 
  			TABLOCK
			  );
        SET @End_Time = GETDATE();
        PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

        -- crm_prd_info
        SET @Start_Time = GETDATE();
        PRINT 'bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\kandr\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',', 
				TABLOCK);
        SET @End_Time = GETDATE();
        PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

        -- crm_sales_details
        SET @Start_Time = GETDATE();
        PRINT 'bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\kandr\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',', 
				TABLOCK
				);
        SET @End_Time = GETDATE();
        PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

        -- ERP TABLES
        PRINT '--------------------------------------------';
        PRINT 'ERP TABLES';
        PRINT '--------------------------------------------';

        -- erp_cust_az12
        SET @Start_Time = GETDATE();
        PRINT 'bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\kandr\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',', 
				TABLOCK
				);
        SET @End_Time = GETDATE();
        PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

        -- erp_loc_a101
        SET @Start_Time = GETDATE();
        PRINT 'bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\kandr\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',', 
				TABLOCK
				);
        SET @End_Time = GETDATE();
        PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

        -- erp_px_cat_g1v2
        SET @Start_Time = GETDATE();
        PRINT 'bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\kandr\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
  			FIRSTROW = 2, 
  			FIELDTERMINATOR = ',', 
  			TABLOCK
  			);
        SET @End_Time = GETDATE();
        PRINT '>> Loading Time: ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) AS NVARCHAR) + ' Seconds';

    END TRY
    BEGIN CATCH
        PRINT 'ERROR Message: ' + ERROR_MESSAGE();
        PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
    END CATCH;

    PRINT '============================================';
    PRINT 'Total Load Time: ' + CAST(DATEDIFF(SECOND,@Proc_Start_Time,GETDATE()) AS NVARCHAR) + ' Seconds';
    PRINT '============================================';
END;
