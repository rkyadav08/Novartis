
CREATE OR ALTER PROCEDURE silver.load_cpid_edc_crf_unlocked
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
        PRINT 'Dataset: CPID EDC CRF Unlocked';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.cpid_edc_crf_unlocked';
        TRUNCATE TABLE silver.cpid_edc_crf_unlocked;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data into silver.cpid_edc_crf_unlocked';

        INSERT INTO silver.cpid_edc_crf_unlocked (
            study_id,
            region,
            country,
            site_id,
            subject_id,
            page_name,
            lock_unlock_status,
            visit_date,
            dwh_create_date
        )
        SELECT
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,
            LTRIM(RTRIM(ISNULL(region, 'NA'))) AS region,
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,
            CASE 
                WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA'
                WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id))
                ELSE 'Site ' + LTRIM(RTRIM(site_id))
            END AS site_id,
            CASE 
                WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA'
                WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id))
                ELSE 'Subject ' + LTRIM(RTRIM(subject_id))
            END AS subject_id,
            LTRIM(RTRIM(ISNULL(page_name, 'NA'))) AS page_name,
            LTRIM(RTRIM(ISNULL(lock_unlock_status, 'NA'))) AS lock_unlock_status,
            
            -- Visit Date: Handle multiple date formats
            CASE
                WHEN visit_date_raw IS NULL OR LTRIM(RTRIM(visit_date_raw)) = '' THEN NULL
                -- Try format: 1/25/2023 (M/D/YYYY or MM/DD/YYYY)
                WHEN visit_date_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, visit_date_raw, 101)
                -- Try format: 03-01-23 (MM-DD-YY)
                WHEN visit_date_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(visit_date_raw))) <= 10 
                     AND RIGHT(visit_date_raw, 2) < '50' THEN TRY_CONVERT(DATE, visit_date_raw, 10)
                -- Try format: 8/22/2023 or similar variations
                WHEN ISNUMERIC(LEFT(visit_date_raw, 1)) = 1 THEN TRY_CONVERT(DATE, visit_date_raw, 101)
                -- Try DD-Mon-YYYY format (15 SEP 2025)
                ELSE TRY_CONVERT(DATE, visit_date_raw, 106)
            END AS visit_date,
            
            GETDATE() AS dwh_create_date
        FROM bronze.cpid_edc_crf_unlocked
        WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR);
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'CPID EDC CRF Unlocked Load Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING CPID EDC CRF UNLOCKED LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
GO