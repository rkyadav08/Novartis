
CREATE OR ALTER PROCEDURE silver.load_cpid_edc_sdv
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME,
        @rows_affected     INT;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT 'Dataset: CPID EDC SDV';
        PRINT '================================================';
        PRINT '>> Truncating Table: silver.cpid_edc_sdv';
        TRUNCATE TABLE silver.cpid_edc_sdv;
        SET @start_time = GETDATE();
        PRINT '>> Inserting cleaned data into silver.cpid_edc_sdv';
        INSERT INTO silver.cpid_edc_sdv (
            study_id,
            region,
            country,
            site_id,
            subject_id,
            folder_name,
            data_page_name,
            visit_date,
            verification_status,
            dwh_create_date
        )
        SELECT
            -- Study ID: Standardize
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,
            -- Region: Clean
            LTRIM(RTRIM(ISNULL(region, 'NA'))) AS region,
            -- Country: Clean
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,
            -- Site ID: Normalize
            CASE 
                WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA'
                WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id))
                ELSE 'Site ' + LTRIM(RTRIM(site_id))
            END AS site_id,
            -- Subject ID: Normalize
            CASE 
                WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA'
                WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id))
                ELSE 'Subject ' + LTRIM(RTRIM(subject_id))
            END AS subject_id,
            -- Folder Name: Clean
            LTRIM(RTRIM(ISNULL(folder_name, 'NA'))) AS folder_name,
            -- Data Page Name: Clean
            LTRIM(RTRIM(ISNULL(data_page_name, 'NA'))) AS data_page_name,
            -- Visit Date: Convert to DATE with flexible format handling
            CASE
                WHEN visit_date_raw IS NULL THEN NULL
                WHEN UPPER(LTRIM(RTRIM(visit_date_raw))) = 'NULL' THEN NULL
                WHEN LTRIM(RTRIM(visit_date_raw)) = '' THEN NULL
                -- Try M/D/YYYY or MM/DD/YYYY format (5/20/2022, 11/11/2022, 2/2/2022)
                WHEN visit_date_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, visit_date_raw, 101)
                -- Try MM-DD-YY format (03-01-23)
                WHEN visit_date_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(visit_date_raw))) <= 10 
                     THEN TRY_CONVERT(DATE, visit_date_raw, 10)
                -- Try DD-Mon-YYYY format (15 SEP 2025)
                ELSE TRY_CONVERT(DATE, visit_date_raw, 106)
            END AS visit_date,
            -- Verification Status: Clean
            LTRIM(RTRIM(ISNULL(verification_status, 'NA'))) AS verification_status,
            GETDATE() AS dwh_create_date
        FROM bronze.cpid_edc_sdv
        WHERE study_id IS NOT NULL;
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR);
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'CPID EDC SDV Load Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING CPID EDC SDV LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
        THROW;
    END CATCH
END;