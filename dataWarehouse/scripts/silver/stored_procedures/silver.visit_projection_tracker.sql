CREATE OR ALTER PROCEDURE silver.load_visit_projection_tracker
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
        PRINT 'Dataset: Visit Projection Tracker';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.visit_projection_tracker';
        TRUNCATE TABLE silver.visit_projection_tracker;

        PRINT '>> Inserting cleaned data into silver.visit_projection_tracker';

        INSERT INTO silver.visit_projection_tracker
        (
            study_id,
            country,
            site_id,
            subject_id,
            visit,
            projected_date,
            days_outstanding
        )
        SELECT
            study_id,
            country,
            site_id,
            subject_id,
            visit,

            -- Convert projected_date (e.g. 09SEP2025 / 22OCT2025)
            TRY_CONVERT(DATE, projected_date, 106) AS projected_date,

            days_outstanding
        FROM bronze.visit_projection_tracker;

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
