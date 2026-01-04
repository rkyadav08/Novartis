CREATE OR ALTER PROCEDURE bronze.load_sae_dashboard_safety_csv
AS
BEGIN
    DECLARE 
        @file_path         NVARCHAR(500),
        @sql               NVARCHAR(MAX),
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;
	IF CURSOR_STATUS('local', 'file_cursor') >= -1
	BEGIN 
		CLOSE file_cursor;
		DEALLOCATE file_cursor;
	END

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading globalcodingreport_meddra CSV Files into Bronze';
        PRINT 'Source Folder: C:\DataWarehouseNovartis\sae_dashboard_safety';
        PRINT '================================================';
		PRINT '>> Truncating Table: bronze.sae_dashboard_safety';
		TRUNCATE TABLE bronze.sae_dashboard_safety;

        /* Temporary table to hold file names */
        CREATE TABLE #csv_files (
            file_name NVARCHAR(255)
        );

        INSERT INTO #csv_files
        EXEC xp_cmdshell 'dir /b C:\DataWarehouseNovartis\sae_dashboard_safety\*.csv';

        DECLARE file_cursor CURSOR FOR
        SELECT 
            'C:\DataWarehouseNovartis\sae_dashboard_safety\' + file_name
        FROM #csv_files
        WHERE file_name IS NOT NULL;

        OPEN file_cursor;
        FETCH NEXT FROM file_cursor INTO @file_path;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @start_time = GETDATE();
            PRINT '>> Loading File: ' + @file_path;

            SET @sql = '
                BULK INSERT bronze.sae_dashboard_safety
                FROM ''' + @file_path + '''
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '','',
                    ROWTERMINATOR = ''0x0d0a'',
                    TABLOCK,
                    CODEPAGE = ''65001''
                );';

            EXEC sp_executesql @sql;

            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
                + ' seconds';
            PRINT '>> --------------------------------------------';

            FETCH NEXT FROM file_cursor INTO @file_path;
        END;

        CLOSE file_cursor;
        DEALLOCATE file_cursor;
        DROP TABLE #csv_files;

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'sae_dashboard_safety Bronze Load Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING sae_dashboard_safety LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
