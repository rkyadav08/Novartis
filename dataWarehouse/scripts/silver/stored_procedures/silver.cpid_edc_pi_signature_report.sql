CREATE OR ALTER PROCEDURE silver.load_cpid_edc_pi_signature_report
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME, @rows_affected INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer: CPID EDC PI Signature Report';
        PRINT '================================================';
        TRUNCATE TABLE silver.cpid_edc_pi_signature_report;
        SET @start_time = GETDATE();

        INSERT INTO silver.cpid_edc_pi_signature_report (study_id, region, country, site_id, subject_id, visit_name, form_name, page_require_signature, audit_action, visit_date, date_last_pi_sign, no_of_days, pending_since_pi_signed, dwh_create_date)
        SELECT
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,
            LTRIM(RTRIM(ISNULL(region, 'NA'))) AS region,
            LTRIM(RTRIM(ISNULL(country, 'NA'))) AS country,
            CASE WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA' WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id)) ELSE 'Site ' + LTRIM(RTRIM(site_id)) END AS site_id,
            CASE WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA' WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id)) ELSE 'Subject ' + LTRIM(RTRIM(subject_id)) END AS subject_id,
            LTRIM(RTRIM(ISNULL(visit_name, 'NA'))) AS visit_name,
            LTRIM(RTRIM(ISNULL(form_name, 'NA'))) AS form_name,
            LTRIM(RTRIM(ISNULL(page_require_signature, 'NA'))) AS page_require_signature,
            LTRIM(RTRIM(ISNULL(audit_action, 'NA'))) AS audit_action,
            CASE WHEN visit_date_raw IS NULL OR LTRIM(RTRIM(visit_date_raw)) = '' THEN NULL WHEN visit_date_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, visit_date_raw, 101) WHEN visit_date_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(visit_date_raw))) <= 10 THEN TRY_CONVERT(DATE, visit_date_raw, 10) ELSE TRY_CONVERT(DATE, visit_date_raw, 106) END AS visit_date,
            CASE WHEN date_last_pi_sign_raw IS NULL OR LTRIM(RTRIM(date_last_pi_sign_raw)) = '' THEN NULL WHEN date_last_pi_sign_raw LIKE '%/%/%' THEN TRY_CONVERT(DATE, date_last_pi_sign_raw, 101) WHEN date_last_pi_sign_raw LIKE '%-%-%' AND LEN(LTRIM(RTRIM(date_last_pi_sign_raw))) <= 10 THEN TRY_CONVERT(DATE, date_last_pi_sign_raw, 10) ELSE TRY_CONVERT(DATE, date_last_pi_sign_raw, 106) END AS date_last_pi_sign,
            ISNULL(no_of_days, 0) AS no_of_days,
            LTRIM(RTRIM(ISNULL(pending_since_pi_signed, 'NA'))) AS pending_since_pi_signed,
            GETDATE() AS dwh_create_date
        FROM bronze.cpid_edc_pi_signature_report WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO