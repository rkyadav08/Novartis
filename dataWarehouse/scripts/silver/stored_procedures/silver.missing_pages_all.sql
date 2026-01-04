CREATE OR ALTER PROCEDURE silver.load_missing_pages_all
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT 'Dataset: Missing Pages - All';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.missing_pages_all';
        TRUNCATE TABLE silver.missing_pages_all;

        PRINT '>> Inserting cleaned data into silver.missing_pages_all';

        INSERT INTO silver.missing_pages_all
        (
            study_id,
            site_group,
            site_id,
            subject_id,
            overall_subject_status,
            visit_level_subject_status,
            folder_name,
            page_name,
            visit_date,
            days_page_missing
        )
        SELECT
            study_id,
            ISNULL(site_group, 'NA') AS site_group,
            site_id,
            subject_id,

            ISNULL(overall_subject_status, 'NA') AS overall_subject_status,
            ISNULL(visit_level_subject_status, 'NA') AS visit_level_subject_status,

            ISNULL(folder_name, 'NA') AS folder_name,
            ISNULL(page_name, 'NA') AS page_name,

            /* ---- visit_date handling ---- */
            CASE
                WHEN visit_date_raw IS NULL THEN NULL
                WHEN LTRIM(RTRIM(UPPER(visit_date_raw))) = 'MISSING VISIT DATE' THEN NULL
                ELSE TRY_CONVERT(DATE, visit_date_raw, 106)
            END AS visit_date,
            days_page_missing

        FROM bronze.missing_pages_all 
		WHERE study_id IS NOT NULL;

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'Silver Load Completed Successfully';
        PRINT 'Total Load Duration: '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
