CREATE OR ALTER PROCEDURE silver.load_inactivated_forms_loglines
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
        PRINT 'Dataset: Inactivated Forms and Loglines';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.inactivated_forms_loglines';
        TRUNCATE TABLE silver.inactivated_forms_loglines;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data into silver.inactivated_forms_loglines';

        INSERT INTO silver.inactivated_forms_loglines (
            study_id,
            country,
            site_id,
            subject_id,
            folder,
            form,
            data_on_form,
            record_position,
            audit_action,
            dwh_create_date
        )
        SELECT
            -- Study ID: Standardize
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,

            -- Country: Clean
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,

            -- Site ID: Normalize (may contain site name in this dataset)
            CASE 
                WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA'
                WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id))
                WHEN ISNUMERIC(LTRIM(RTRIM(site_id))) = 1 THEN 'Site ' + LTRIM(RTRIM(site_id))
                ELSE LTRIM(RTRIM(site_id))  -- Keep site name as-is
            END AS site_id,

            -- Subject ID: Normalize
            CASE 
                WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA'
                WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id))
                ELSE 'Subject ' + LTRIM(RTRIM(subject_id))
            END AS subject_id,

            -- Folder: Clean
            LTRIM(RTRIM(ISNULL(folder, 'NA'))) AS folder,

            -- Form: Clean
            LTRIM(RTRIM(ISNULL(form, 'NA'))) AS form,

            -- Data on Form: Standardize Y/N
            CASE 
                WHEN UPPER(LTRIM(RTRIM(data_on_form))) IN ('Y', 'YES', '1', 'TRUE') THEN 'Y'
                WHEN UPPER(LTRIM(RTRIM(data_on_form))) IN ('N', 'NO', '0', 'FALSE') THEN 'N'
                ELSE 'NA'
            END AS data_on_form,

            -- Record Position: Convert to INT
            ISNULL(TRY_CAST(record_position AS INT), 0) AS record_position,

            -- Audit Action: Clean and truncate if needed
            LEFT(LTRIM(RTRIM(ISNULL(audit_action, 'NA'))), 100) AS audit_action,

            GETDATE() AS dwh_create_date

        FROM bronze.inactivated_forms_loglines
        WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR);
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'Inactivated Forms Loglines Load Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING INACTIVATED FORMS LOGLINES LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
GO
