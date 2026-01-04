CREATE OR ALTER PROCEDURE silver.load_compiled_edrr
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading silver.compiled_edrr';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.compiled_edrr';
        TRUNCATE TABLE silver.compiled_edrr;

        PRINT '>> Inserting Data Into: silver.compiled_edrr';
        INSERT INTO silver.compiled_edrr (
            study_id,
            subject_id,
            total_open_issue_count_per_subject
        )
        SELECT
            study_id,
            CASE 
				WHEN LTRIM(UPPER(subject_id)) LIKE 'SUBJECT%' THEN (subject_id)
				ELSE 'Subject NA'
			END AS subject_id,
            total_open_issue_count_per_subject
        FROM bronze.compiled_edrr;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '>> --------------------------------------------';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;  
    END CATCH
END;
