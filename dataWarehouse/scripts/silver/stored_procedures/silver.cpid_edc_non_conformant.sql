CREATE OR ALTER PROCEDURE exsilver.load_cpid_edc_non_conformant
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME, @rows_affected INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer: CPID EDC Non Conformant';
        PRINT '================================================';
        TRUNCATE TABLE silver.cpid_edc_non_conformant;
        SET @start_time = GETDATE();

        INSERT INTO  silver.cpid_edc_non_conformant (study_id, region, country, site_id, subject_id, folder_name, page_name, log_no, field_oid, audit_time, visit_date, dwh_create_date)
        SELECT
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,
            LTRIM(RTRIM(ISNULL(region, 'NA'))) AS region,
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,
            CASE WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA' WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id)) ELSE 'Site ' + LTRIM(RTRIM(site_id)) END AS site_id,
            CASE WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA' WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id)) ELSE 'Subject ' + LTRIM(RTRIM(subject_id)) END AS subject_id,
            LTRIM(RTRIM(ISNULL(folder_name, 'NA'))) AS folder_name,
            LTRIM(RTRIM(ISNULL(page_name, 'NA'))) AS page_name,
            ISNULL(log_no, 0) AS log_no,
            LTRIM(RTRIM(ISNULL(field_oid, 'NA'))) AS field_oid,
            TRY_CONVERT(DATETIME, audit_time_raw, 121) AS audit_time,
            CASE WHEN visit_date_raw IS NULL OR LTRIM(RTRIM(visit_date_raw)) = '' THEN NULL WHEN visit_date_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, visit_date_raw, 101) WHEN visit_date_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(visit_date_raw))) <= 10 THEN TRY_CONVERT(DATE, visit_date_raw, 10) ELSE TRY_CONVERT(DATE, visit_date_raw, 106) END AS visit_date,
            GETDATE() AS dwh_create_date
        FROM bronze.cpid_edc_non_conformant WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO