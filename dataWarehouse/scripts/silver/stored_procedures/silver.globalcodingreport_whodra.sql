CREATE OR ALTER PROCEDURE silver.load_globalcodingreport_whodra
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
        PRINT 'Dataset: Global Coding Report WHO-DRA';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.globalcodingreport_whodra';
        TRUNCATE TABLE silver.globalcodingreport_whodra;

        PRINT '>> Inserting cleaned data into silver.globalcodingreport_whodra';

        INSERT INTO silver.globalcodingreport_whodra
        (
            study_id,
            dictionary,
            dictionary_version,
            subject_id,
            form_oid,
            logline,
            field_oid,
            coding_status,
            require_coding
        )
        SELECT
            study_id,
            dictionary,

            --  ONLY transformation: NULL dictionary_version → 'NA'
            ISNULL(dictionary_version, 'NA') AS dictionary_version,

            subject_id,
            form_oid,
            TRY_CAST(logline AS INT) AS logline,
            field_oid,
            coding_status,
            require_coding
        FROM bronze.globalcodingreport_whodra;

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
