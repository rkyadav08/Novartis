CREATE OR ALTER PROCEDURE  silver.load_missing_pages_visit_level
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME,
        @rows_affected     INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT 'Dataset: Missing Pages - Visit Level';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.missing_pages_visit_level';
        TRUNCATE TABLE silver.missing_pages_visit_level;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data into silver.missing_pages_visit_level';

        INSERT INTO silver.missing_pages_visit_level (
            study_id,
            site_group,
            site_id,
            subject_id,
            overall_subject_status,
            visit_level_subject_status,
            form_subject_status,
            visit_name,
            folder_name,
            form_name,
            visit_date,
            days_page_missing,
            dwh_create_date
        )
        SELECT
            -- Study ID: Standardize
            UPPER(LTRIM(RTRIM(study_id))) AS study_id,

            -- Site Group (Country): Clean
            CASE UPPER(LTRIM(RTRIM(site_group)))
    WHEN 'ISR' THEN 'ISR'
    WHEN 'ISRAEL' THEN 'ISR'

    WHEN 'CHN' THEN 'CHN'
    WHEN 'CHINA' THEN 'CHN'

    WHEN 'JPN' THEN 'JPN'

    WHEN 'USA' THEN 'USA'
    WHEN 'UNITED STATES OF AMERICA' THEN 'USA'

    WHEN 'CAN' THEN 'CAN'
    WHEN 'CANADA' THEN 'CAN'

    WHEN 'ARG' THEN 'ARG'
    WHEN 'ARGENTINA' THEN 'ARG'

    WHEN 'AUS' THEN 'AUS'
    WHEN 'AUSTRALIA' THEN 'AUS'

    WHEN 'ITALY' THEN 'ITA'
    WHEN 'ITA' THEN 'ITA'

    WHEN 'BRAZIL' THEN 'BRA'
    WHEN 'BRA' THEN 'BRA'

    WHEN 'DEU' THEN 'DEU'

    WHEN 'NETHERLANDS' THEN 'NLD'
    WHEN 'NLD' THEN 'NLD'

    WHEN 'SVK' THEN 'SVK'
    WHEN 'CHE' THEN 'CHE'
    WHEN 'MYS' THEN 'MYS'

    WHEN 'CZE' THEN 'CZE'
    WHEN 'CZECHIA' THEN 'CZE'

    WHEN 'LITHUANIA' THEN 'LTU'
    WHEN 'ICELAND' THEN 'ISL'

    WHEN 'KOR' THEN 'KOR'
    WHEN 'KOREA (THE REPUBLIC OF)' THEN 'KOR'

    WHEN 'HUNGARY' THEN 'HUN'
    WHEN 'HUN' THEN 'HUN'

    WHEN 'MAURITIUS' THEN 'MUS'

    WHEN 'POL' THEN 'POL'
    WHEN 'POLAND' THEN 'POL'

    WHEN 'AUT' THEN 'AUT'
    WHEN 'MEXICO' THEN 'MEX'

    WHEN 'BEL' THEN 'BEL'
    WHEN 'BELGIUM' THEN 'BEL'

    WHEN 'BFA' THEN 'BFA'
    WHEN 'SERBIA' THEN 'SRB'

    WHEN 'INDIA' THEN 'IND'
    WHEN 'CROATIA' THEN 'HRV'
    WHEN 'SGP' THEN 'SGP'
    WHEN 'BULGARIA' THEN 'BGR'

    WHEN 'SWE' THEN 'SWE'
    WHEN 'TWN' THEN 'TWN'
    WHEN 'LATVIA' THEN 'LVA'

    WHEN 'FRA' THEN 'FRA'
    WHEN 'FRANCE' THEN 'FRA'

    WHEN 'THAILAND' THEN 'THA'
    WHEN 'SPAIN' THEN 'ESP'
    WHEN 'ESP' THEN 'ESP'

    WHEN 'GBR' THEN 'GBR'
    WHEN 'TURKEY' THEN 'TUR'
    WHEN 'DENMARK' THEN 'DNK'
    WHEN 'COLOMBIA' THEN 'COL'
    WHEN 'CHILE' THEN 'CHL'
    WHEN 'SOUTH AFRICA' THEN 'ZAF'
    WHEN 'PORTUGAL' THEN 'PRT'

    ELSE 'NA'
END AS site_group,


            -- Site ID: Normalize
            CASE 
                WHEN site_id IS NULL OR LTRIM(RTRIM(site_id)) = '' THEN 'Site NA'
                WHEN UPPER(LTRIM(site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(site_id))
                ELSE 'Site ' + LTRIM(RTRIM(site_id))
            END AS site_id,

            -- Subject ID: Normalize
            CASE 
                WHEN subject_id IS NULL OR LTRIM(RTRIM(subject_id)) = '' THEN 'Subject NA'
                WHEN UPPER(LTRIM(subject_id)) LIKE 'SUBJECT%' THEN LTRIM(RTRIM(subject_id))
                ELSE 'Subject ' + LTRIM(RTRIM(subject_id))
            END AS subject_id,

            -- Overall Subject Status
            LTRIM(RTRIM(ISNULL(overall_subject_status, 'NA'))) AS overall_subject_status,

            -- Visit Level Subject Status
            LTRIM(RTRIM(ISNULL(visit_level_subject_status, 'NA'))) AS visit_level_subject_status,

            -- Form Subject Status
            LTRIM(RTRIM(ISNULL(form_subject_status, 'NA'))) AS form_subject_status,

            -- Visit Name
            LTRIM(RTRIM(ISNULL(visit_name, 'NA'))) AS visit_name,

            -- Folder Name
            LTRIM(RTRIM(ISNULL(folder_name, 'NA'))) AS folder_name,

            -- Form Name
            LTRIM(RTRIM(ISNULL(form_name, 'NA'))) AS form_name,

            -- Visit Date: Convert to DATE
            CASE
                WHEN visit_date IS NULL OR LTRIM(RTRIM(visit_date)) = '' THEN NULL
                WHEN UPPER(LTRIM(RTRIM(visit_date))) = 'MISSING VISIT DATE' THEN NULL
                ELSE TRY_CONVERT(DATE, visit_date, 106)  -- Format: DD-Mon-YYYY
            END AS visit_date,

            -- Days Page Missing: Keep as-is (already INT in bronze)
            ISNULL(days_page_missing, 0) AS days_page_missing,

            GETDATE() AS dwh_create_date

        FROM bronze.missing_pages_visit_level
        WHERE study_id IS NOT NULL;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR);
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
            + ' seconds';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'Missing Pages Visit Level Load Completed Successfully';
        PRINT 'Total Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
            + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING MISSING PAGES VISIT LEVEL LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
GO
