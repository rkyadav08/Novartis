CREATE OR ALTER PROCEDURE silver.load_cpid_edc_query_report_site_action
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME, @rows_affected INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer: CPID EDC Query Report Site Action';
        PRINT '================================================';
        TRUNCATE TABLE silver.cpid_edc_query_report_site_action;
        SET @start_time = GETDATE();

        INSERT INTO silver.cpid_edc_query_report_site_action (study_id, region, country, site_id, subject_id, folder_name, form_name, field_oid, log_no, visit_date, query_status, action_owner, marking_group_name, query_open_date, query_response, days_since_open, days_since_response, dwh_create_date)
        SELECT
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,
            LTRIM(RTRIM(ISNULL(region, 'NA'))) AS region,
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,
            CASE WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA' WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id)) ELSE 'Site ' + LTRIM(RTRIM(site_id)) END AS site_id,
            CASE WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA' WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id)) ELSE 'Subject ' + LTRIM(RTRIM(subject_id)) END AS subject_id,
            LTRIM(RTRIM(ISNULL(folder_name, 'NA'))) AS folder_name,
            LTRIM(RTRIM(ISNULL(form_name, 'NA'))) AS form_name,
            LTRIM(RTRIM(ISNULL(field_oid, 'NA'))) AS field_oid,
            ISNULL(log_no, 0) AS log_no,
            CASE WHEN visit_date_raw IS NULL OR LTRIM(RTRIM(visit_date_raw)) = '' THEN NULL WHEN visit_date_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, visit_date_raw, 101) WHEN visit_date_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(visit_date_raw))) <= 10 THEN TRY_CONVERT(DATE, visit_date_raw, 10) ELSE TRY_CONVERT(DATE, visit_date_raw, 106) END AS visit_date,
            LTRIM(RTRIM(ISNULL(query_status, 'NA'))) AS query_status,
            LTRIM(RTRIM(ISNULL(action_owner, 'NA'))) AS action_owner,
            LTRIM(RTRIM(ISNULL(marking_group_name, 'NA'))) AS marking_group_name,
            CASE WHEN query_open_date_raw IS NULL OR LTRIM(RTRIM(query_open_date_raw)) = '' THEN NULL WHEN query_open_date_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, query_open_date_raw, 101) WHEN query_open_date_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(query_open_date_raw))) <= 10 THEN TRY_CONVERT(DATE, query_open_date_raw, 10) ELSE TRY_CONVERT(DATE, query_open_date_raw, 106) END AS query_open_date,
            LTRIM(RTRIM(ISNULL(query_response, 'NA'))) AS query_response,
            ISNULL(days_since_open, 0) AS days_since_open,
            ISNULL(days_since_response, 0) AS days_since_response,
            GETDATE() AS dwh_create_date
        FROM bronze.cpid_edc_query_report_site_action WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO