CREATE OR ALTER PROCEDURE silver.load_sae_dashboard_dm
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
        PRINT 'Loading Silver Data Mart';
        PRINT 'Dataset: SAE Dashboard DM';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.sae_dashboard_dm';
        TRUNCATE TABLE silver.sae_dashboard_dm;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data into silver.sae_dashboard_dm';

        INSERT INTO silver.sae_dashboard_dm (
            discrepancy_id,
            study_id,
            country,
            site_id,
            patient_id,
            form_name,
            discrepancy_ts,
            review_status,
            action_status,
            dwh_create_date
        )
        SELECT
            discrepancy_id,
            study_id,
            country,

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

            -- Normalize form_name (coming from case_status)
            ISNULL(
                NULLIF(REPLACE(form_name, '-', ''), ''),
                'NA'
            ) AS form_name,

            -- Convert discrepancy timestamp
            ISNULL(TRY_CONVERT(DATETIME, discrepancy_ts, 113),getdate()) AS discrepancy_ts,

            -- Normalize review_status
            ISNULL(NULLIF(LTRIM(RTRIM(review_status)), ''), 'NA') AS review_status,

            -- Normalize action_status
            ISNULL(
                NULLIF(LTRIM(RTRIM(RTRIM(action_status, ','),'-')), ''),
                'NA'
            ) AS action_status,

            GETDATE() AS dwh_create_date

        FROM bronze.sae_dashboard_dm
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
        PRINT 'SAE Dashboard DM Load Completed Successfully';
        PRINT 'Total Load Duration: '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING SAE DASHBOARD DM LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
