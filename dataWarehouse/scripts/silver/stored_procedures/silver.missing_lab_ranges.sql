CREATE OR ALTER PROCEDURE silver.load_missing_lab_ranges
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
        PRINT 'Dataset: Missing Lab Ranges';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.missing_lab_ranges';
        TRUNCATE TABLE silver.missing_lab_ranges;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data into silver.missing_lab_ranges';

        INSERT INTO silver.missing_lab_ranges (
            study_id,
            country,
            site_id,
            subject_id,
            visit,
            form_name,
            lab_category,
            lab_date,
            test_name,
            test_description,
            issue,
            comments,
            dwh_create_date
        )
        SELECT
            -- Study ID: Standardize
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,

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

            -- Visit: Clean
            LTRIM(RTRIM(ISNULL(visit, 'NA'))) AS visit,

            -- Form Name: Standardize
            LTRIM(RTRIM(ISNULL(form_name, 'NA'))) AS form_name,

            -- Lab Category: Standardize to uppercase
            UPPER(LTRIM(RTRIM(ISNULL(lab_category, 'NA')))) AS lab_category,

            -- Lab Date: Convert to DATE type
            CASE
                WHEN lab_date IS NULL OR LTRIM(RTRIM(lab_date)) = '' THEN NULL
                ELSE TRY_CONVERT(DATE, lab_date, 106)  -- Format: DD-Mon-YYYY
            END AS lab_date,

            -- Test Name: Clean
            LTRIM(RTRIM(ISNULL(test_name, 'NA'))) AS test_name,

            -- Test Description: Clean
            LTRIM(RTRIM(ISNULL(test_description, 'NA'))) AS test_description,

            -- Issue: Standardize
            LTRIM(RTRIM(ISNULL(issue, 'NA'))) AS issue,

            -- Comments: Clean
            LTRIM(RTRIM(ISNULL(comments, ''))) AS comments,

            GETDATE() AS dwh_create_date

        FROM bronze.missing_lab_ranges
        WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR);
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'Missing Lab Ranges Load Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING MISSING LAB RANGES LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
GO
