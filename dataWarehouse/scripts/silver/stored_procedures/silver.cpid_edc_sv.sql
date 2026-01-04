CREATE OR ALTER PROCEDURE silver.load_cpid_edc_sv
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME, @rows_affected INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer: CPID EDC SV';
        PRINT '================================================';
        TRUNCATE TABLE silver.cpid_edc_sv;
        SET @start_time = GETDATE();

        INSERT INTO silver.cpid_edc_sv (project_name, region, country, site_id, subject_name, folder_name, visit_date, dwh_create_date)
        SELECT
            UPPER(LTRIM(RTRIM(project_name))) AS project_name,
            LTRIM(RTRIM(ISNULL(region, 'NA'))) AS region,
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,
            CASE WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA' WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id)) ELSE 'Site ' + LTRIM(RTRIM(site_id)) END AS site_id,
            CASE WHEN subject_name IS NULL OR LTRIM(RTRIM(subject_name)) = '' THEN 'Subject NA' WHEN UPPER(LTRIM(subject_name)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_name)) ELSE 'Subject ' + LTRIM(RTRIM(subject_name)) END AS subject_name,
            LTRIM(RTRIM(ISNULL(folder_name, 'NA'))) AS folder_name,
            CASE WHEN visit_date IS NULL OR LTRIM(RTRIM(visit_date)) = '' THEN NULL WHEN visit_date LIKE '%/%/%' THEN TRY_CONVERT(DATE, visit_date, 101) WHEN visit_date LIKE '%-%-%' AND LEN(LTRIM(RTRIM(visit_date))) <= 10 THEN TRY_CONVERT(DATE, visit_date, 10) ELSE TRY_CONVERT(DATE, visit_date, 106) END AS visit_date,
            GETDATE() AS dwh_create_date
        FROM bronze.cpid_edc_sv WHERE project_name IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO