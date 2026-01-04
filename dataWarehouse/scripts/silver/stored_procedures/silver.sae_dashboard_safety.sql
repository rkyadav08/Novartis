CREATE OR ALTER PROCEDURE silver.load_sae_dashboard_safety
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT 'Dataset: SAE Dashboard Safety';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.sae_dashboard_safety';
        TRUNCATE TABLE silver.sae_dashboard_safety;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data into silver.sae_dashboard_safety';

        INSERT INTO silver.sae_dashboard_safety (
            discrepancy_id,
            study_id,
            site_id,
            patient_id,
            case_status,
            discrepancy_ts,
            review_status,
            action_status
        )
        SELECT
            discrepancy_id,
            study_id,

            -- Normalize site_id
            CASE 
                WHEN LTRIM(UPPER(site_id)) LIKE 'SITE%' 
                    THEN site_id
                ELSE 'Site NA'
            END AS site_id,

            -- Normalize patient_id
            CASE 
                WHEN LTRIM(UPPER(patient_id)) LIKE 'SUBJECT%' 
                    THEN patient_id
                ELSE 'Subject NA'
            END AS patient_id,

            -- Remove all dashes from case_status
            REPLACE(case_status, '-', 'NA') AS case_status,

            -- Convert timestamp
            TRY_CONVERT(DATETIME, discrepancy_ts, 113) AS discrepancy_ts,

            review_status,

            -- Blank or NULL → NA, also remove trailing commas
            ISNULL(
                NULLIF(
                    LTRIM(RTRIM(RTRIM(action_status, ','))), 
                    ''
                ),
                'NA'
            ) AS action_status

        FROM bronze.sae_dashboard_safety
        WHERE NOT (
            discrepancy_id IS NULL
            AND study_id IS NULL
        );

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';

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
