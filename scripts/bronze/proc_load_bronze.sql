/*
-----------------------------------------------------------
Stored Procedure: Load Bronze Layer (Source -> Bronze)
-----------------------------------------------------------

Script Purpose:
          This stored procedure loads data into the 'bronze' schema from external CSV files.
          It performs the following actions:
          - Truncates the bronze tables before loading data.
          - Uses the BULK INSERT command to load data from CSV files to bronze tables.

Parameters:
          None.
          This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
-----------------------------------------------------------
*/


-- ====================================

--EXEC bronze.load_bronze 
--this line is the execution line

-- ====================================
 
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=====================================';
        PRINT '        Loading Bronze Layer          ';
        PRINT '=====================================';

        PRINT '-------------------------------------';
        PRINT '        Loading CRM Tables            ';
        PRINT '-------------------------------------';

        ---------------- CRM_CUST_INFO ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into : bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) AS [RowCount] FROM bronze.crm_cust_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';


        ---------------- CRM_PRD_INFO ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into : bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) AS [RowCount] FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';


        ---------------- CRM_SALES_DETAILS ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into : bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) AS [RowCount] FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';


        PRINT '-------------------------------------';
        PRINT '        Loading ERP Tables            ';
        PRINT '-------------------------------------';

        ---------------- ERP_CUST_AZ12 ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_CUST_AZ12';
        TRUNCATE TABLE bronze.erp_CUST_AZ12;

        PRINT '>> Inserting Data Into : bronze.erp_CUST_AZ12';
        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) AS [RowCount] FROM bronze.erp_CUST_AZ12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';


        ---------------- ERP_LOC_A101 ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) AS [RowCount] FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';


        ---------------- ERP_PX_CAT_G1V2 ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_PX_CAT_G1V2';
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

        PRINT '>> Inserting Data Into : bronze.erp_PX_CAT_G1V2';
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) AS [RowCount] FROM bronze.erp_PX_CAT_G1V2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------------------------';


        ------------- FINAL SUMMARY -------------
        SET @batch_end_time = GETDATE();
        PRINT '=====================================';
        PRINT ' Bronze Layer Loading Completed';
        PRINT ' Total Duration : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=====================================';

    END TRY

    BEGIN CATCH
        PRINT '===================================================';
        PRINT ' Error Occurred During Bronze Layer Loading ';
        PRINT ' Error Message    : ' + ERROR_MESSAGE();
        PRINT ' Error Number     : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT ' Error State      : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================================';
    END CATCH
END;
