IF OBJECT_ID('gold.dim_region', 'V') IS NOT NULL
    DROP VIEW gold.dim_region;
GO

CREATE VIEW gold.dim_region AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY region) AS region_key,
    region AS region_code,
    CASE 
        WHEN region = 'ASIA' THEN 'Asia'
        WHEN region = 'EU' THEN 'Europe'
        WHEN region = 'US' THEN 'United States'
        WHEN region = 'LATAM' THEN 'Latin America'
        WHEN region = 'MEA' THEN 'Middle East & Africa'
        ELSE region
    END AS region_name
FROM silver.cpid_edc_subject_metrics
WHERE region IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_country (Level 2 - Standalone with region_code for joining)
-- =============================================================================
IF OBJECT_ID('gold.dim_country', 'V') IS NOT NULL
    DROP VIEW gold.dim_country;
GO

CREATE VIEW gold.dim_country AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY region, country) AS country_key,
    region AS region_code,
    country AS country_code,
    CASE country
        WHEN 'HKG' THEN 'Hong Kong'
        WHEN 'ARG' THEN 'Argentina'
        WHEN 'AUT' THEN 'Austria'
        WHEN 'CHL' THEN 'Chile'
        WHEN 'PRT' THEN 'Portugal'
        WHEN 'MYS' THEN 'Malaysia'
        WHEN 'BFA' THEN 'Burkina Faso'
        WHEN 'THA' THEN 'Thailand'
        WHEN 'GHA' THEN 'Ghana'
        WHEN 'FIN' THEN 'Finland'
        WHEN 'IRL' THEN 'Ireland'
        WHEN 'LVA' THEN 'Latvia'
        WHEN 'DEU' THEN 'Germany'
        WHEN 'ESP' THEN 'Spain'
        WHEN 'RUS' THEN 'Russia'
        WHEN 'KEN' THEN 'Kenya'
        WHEN 'CHE' THEN 'Switzerland'
        WHEN 'CZE' THEN 'Czech Republic'
        WHEN 'NZL' THEN 'New Zealand'
        WHEN 'EST' THEN 'Estonia'
        WHEN 'USA' THEN 'United States'
        WHEN 'PRK' THEN 'North Korea'
        WHEN 'SVN' THEN 'Slovenia'
        WHEN 'ROU' THEN 'Romania'
        WHEN 'POL' THEN 'Poland'
        WHEN 'FRA' THEN 'France'
        WHEN 'ITA' THEN 'Italy'
        WHEN 'NOR' THEN 'Norway'
        WHEN 'BGR' THEN 'Bulgaria'
        WHEN 'TWN' THEN 'Taiwan'
        WHEN 'NLD' THEN 'Netherlands'
        WHEN 'VNM' THEN 'Vietnam'
        WHEN 'ISL' THEN 'Iceland'
        WHEN 'LTU' THEN 'Lithuania'
        WHEN 'HRV' THEN 'Croatia'
        WHEN 'UGA' THEN 'Uganda'
        WHEN 'TUR' THEN 'Turkey'
        WHEN 'AUS' THEN 'Australia'
        WHEN 'CHN' THEN 'China'
        WHEN 'BEL' THEN 'Belgium'
        WHEN 'SRB' THEN 'Serbia'
        WHEN 'IND' THEN 'India'
        WHEN 'LBN' THEN 'Lebanon'
        WHEN 'KOR' THEN 'South Korea'
        WHEN 'MEX' THEN 'Mexico'
        WHEN 'SGP' THEN 'Singapore'
        WHEN 'ISR' THEN 'Israel'
        WHEN 'CAN' THEN 'Canada'
        WHEN 'GAB' THEN 'Gabon'
        WHEN 'JPN' THEN 'Japan'
        WHEN 'GRC' THEN 'Greece'
        WHEN 'HUN' THEN 'Hungary'
        WHEN 'ZAF' THEN 'South Africa'
        WHEN 'SVK' THEN 'Slovakia'
        WHEN 'COL' THEN 'Colombia'
        WHEN 'DNK' THEN 'Denmark'
        WHEN 'BRA' THEN 'Brazil'
        WHEN 'PHL' THEN 'Philippines'
        WHEN 'CIV' THEN 'Ivory Coast'
        WHEN 'GBR' THEN 'United Kingdom'
        WHEN 'SWE' THEN 'Sweden'
        WHEN 'MUS' THEN 'Mauritius'
        ELSE 'Unknown'
    END AS country_name
FROM silver.cpid_edc_subject_metrics
WHERE country IS NOT NULL;

GO

-- =============================================================================
-- Dimension: gold.dim_site (Level 3 - Standalone with country_code for joining)
-- =============================================================================
IF OBJECT_ID('gold.dim_site', 'V') IS NOT NULL
    DROP VIEW gold.dim_site;
GO

CREATE VIEW gold.dim_site AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY region, country, site_id) AS site_key,
    region AS region_code,
    country AS country_code,
    site_id,
    site_id AS site_name,
    'Active' AS site_status
FROM silver.cpid_edc_subject_metrics
WHERE site_id IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_study
-- =============================================================================
IF OBJECT_ID('gold.dim_study', 'V') IS NOT NULL
    DROP VIEW gold.dim_study;
GO

CREATE VIEW gold.dim_study AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY study_id) AS study_key,
    study_id,
    study_id AS study_name,
    'Active' AS study_status
FROM silver.cpid_edc_subject_metrics
WHERE study_id IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_subject (Standalone - uses codes for joining)
-- =============================================================================
IF OBJECT_ID('gold.dim_subject', 'V') IS NOT NULL
    DROP VIEW   gold.dim_subject;
GO

CREATE VIEW gold.dim_subject AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY study_id, site_id, subject_id) AS subject_key,
    study_id,
    site_id,
    region AS region_code,
    country AS country_code,
    subject_id,
    subject_status,
    latest_visit,
    expected_visits
FROM silver.cpid_edc_subject_metrics
WHERE subject_id IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_visit (Visit/Folder Reference)
-- =============================================================================
IF OBJECT_ID('gold.dim_visit', 'V') IS NOT NULL
    DROP VIEW gold.dim_visit;
GO

CREATE VIEW gold.dim_visit AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY folder_name) AS visit_key,
    folder_name AS visit_code,
    folder_name AS visit_name,
    'Scheduled' AS visit_type
FROM (
    SELECT DISTINCT folder_name FROM silver.cpid_edc_sdv WHERE folder_name IS NOT NULL
    UNION
    SELECT DISTINCT folder_name FROM silver.cpid_edc_non_conformant WHERE folder_name IS NOT NULL
    UNION
    SELECT DISTINCT folder_name FROM silver.missing_pages_visit_level WHERE folder_name IS NOT NULL
    UNION
    SELECT DISTINCT visit_name AS folder_name FROM silver.cpid_edc_pi_signature_report WHERE visit_name IS NOT NULL
) visits
WHERE folder_name IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_form (CRF/Form Reference)
-- =============================================================================
IF OBJECT_ID('gold.dim_form', 'V') IS NOT NULL
    DROP VIEW gold.dim_form;
GO

CREATE VIEW gold.dim_form AS
SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY form_name) AS form_key,
    form_name AS form_code,
    form_name AS form_name,
    'CRF' AS form_type
FROM (
    SELECT DISTINCT form_name FROM silver.cpid_edc_query_report_cra_action WHERE form_name IS NOT NULL
    UNION
    SELECT DISTINCT form_name FROM silver.cpid_edc_query_report_site_action WHERE form_name IS NOT NULL
    UNION
    SELECT DISTINCT form_name FROM silver.cpid_edc_pi_signature_report WHERE form_name IS NOT NULL
    UNION
    SELECT DISTINCT form_name FROM silver.missing_lab_ranges WHERE form_name IS NOT NULL
    UNION
    SELECT DISTINCT form_name FROM silver.sae_dashboard_dm WHERE form_name IS NOT NULL
) forms
WHERE form_name IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_query_type (Query Classification - Static)
-- =============================================================================
IF OBJECT_ID('gold.dim_query_type', 'V') IS NOT NULL
    DROP VIEW gold.dim_query_type;
GO

CREATE VIEW gold.dim_query_type AS
SELECT 
    query_type_key,
    query_type_code,
    query_type_name,
    query_category
FROM (
    VALUES 
        (1, 'DM', 'Data Management', 'Operational'),
        (2, 'CLINICAL', 'Clinical', 'Operational'),
        (3, 'MEDICAL', 'Medical', 'Scientific'),
        (4, 'SITE', 'Site', 'Operational'),
        (5, 'FIELD_MONITOR', 'Field Monitor', 'Monitoring'),
        (6, 'CODING', 'Coding', 'Data Quality'),
        (7, 'SAFETY', 'Safety', 'Regulatory')
) AS qt(query_type_key, query_type_code, query_type_name, query_category);
GO

-- =============================================================================
-- Dimension: gold.dim_date (Date Dimension for Time-based Analysis)
-- =============================================================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
SELECT DISTINCT
    CAST(visit_date AS DATE) AS date_key,
    visit_date AS full_date,
    DAY(visit_date) AS day_of_month,
    MONTH(visit_date) AS month_num,
    DATENAME(MONTH, visit_date) AS month_name,
    YEAR(visit_date) AS year_num,
    DATEPART(QUARTER, visit_date) AS quarter_num,
    DATEPART(WEEK, visit_date) AS week_of_year,
    DATENAME(WEEKDAY, visit_date) AS day_name
FROM (
    SELECT visit_date FROM silver.cpid_edc_sdv WHERE visit_date IS NOT NULL
    UNION
    SELECT visit_date FROM silver.cpid_edc_non_conformant WHERE visit_date IS NOT NULL
    UNION
    SELECT visit_date FROM silver.cpid_edc_query_report_cumulative WHERE visit_date IS NOT NULL
    UNION
    SELECT projected_date AS visit_date FROM silver.visit_projection_tracker WHERE projected_date IS NOT NULL
) dates
WHERE visit_date IS NOT NULL;
GO

-- =============================================================================
-- Dimension: gold.dim_action_owner (Responsible Party Reference - Static)
-- =============================================================================
IF OBJECT_ID('gold.dim_action_owner', 'V') IS NOT NULL
    DROP VIEW gold.dim_action_owner;
GO

CREATE VIEW gold.dim_action_owner AS
SELECT 
    owner_key,
    owner_code,
    owner_name,
    owner_category
FROM (
    VALUES 
        (1, 'SITE_CRA', 'Site/CRA', 'Site Operations'),
        (2, 'DM', 'Data Management', 'Data Operations'),
        (3, 'CSE_CDD', 'CSE/CDD', 'Clinical Operations'),
        (4, 'CDMD_MEDICAL', 'CDMD/Medical Lead', 'Medical Review'),
        (5, 'CODER', 'Coder', 'Data Quality'),
        (6, 'SAFETY', 'Safety Team', 'Safety Operations'),
        (7, 'INVESTIGATOR', 'Investigator', 'Site Operations')
) AS ao(owner_key, owner_code, owner_name, owner_category);
GO

-- =============================================================================
-- FACT TABLES (Reference Silver tables directly)
-- =============================================================================

-- =============================================================================
-- Fact Table: gold.fact_subject_metrics (Core Subject-Level Metrics)
-- =============================================================================
IF OBJECT_ID('gold.fact_subject_metrics', 'V') IS NOT NULL
    DROP VIEW gold.fact_subject_metrics;
GO

CREATE VIEW  gold.fact_subject_metrics AS
SELECT
    sm.study_id,
    sm.site_id,
    sm.subject_id,
    sm.region,
    sm.country,
    
    -- Visit Metrics
    sm.missing_visits,
    sm.expected_visits,
    CASE WHEN sm.expected_visits > 0 
        THEN CAST(sm.missing_visits AS FLOAT) / sm.expected_visits * 100 
        ELSE 0 
    END AS pct_missing_visits,
    
    -- Page/CRF Metrics
    sm.missing_pages,
    sm.pages_entered,
    sm.pages_non_conformant,
    sm.crfs_with_issues,
    sm.crfs_clean,
    sm.percent_clean_crf,
    
    -- Coding Metrics
    sm.coded_terms,
    sm.uncoded_terms,
    CASE WHEN (sm.coded_terms + sm.uncoded_terms) > 0 
        THEN CAST(sm.coded_terms AS FLOAT) / (sm.coded_terms + sm.uncoded_terms) * 100 
        ELSE 100 
    END AS pct_coded_terms,
    
    -- Issue Tracking
    sm.open_issues_lnr,
    sm.open_issues_edrr,
    sm.inactivated_forms,
    sm.esae_review_dm,
    sm.esae_review_safety,
    
    -- Query Metrics by Type
    sm.dm_queries,
    sm.clinical_queries,
    sm.medical_queries,
    sm.site_queries,
    sm.field_monitor_queries,
    sm.coding_queries,
    sm.safety_queries,
    sm.total_queries,
    
    -- Verification Status
    sm.crfs_require_verification AS crfs_require_sdv,
    sm.forms_verified,
    CASE WHEN sm.crfs_require_verification > 0 
        THEN CAST(sm.forms_verified AS FLOAT) / sm.crfs_require_verification * 100 
        ELSE 100 
    END AS pct_sdv_complete,
    
    -- Lock/Freeze Status
    sm.crfs_frozen,
    sm.crfs_not_frozen,
    sm.crfs_locked,
    sm.crfs_unlocked,
    
    -- Protocol Deviations
    sm.pds_confirmed,
    sm.pds_proposed,
    
    -- Signature Status
    sm.crfs_signed,
    sm.crfs_overdue_45,
    sm.crfs_overdue_45_90,
    sm.crfs_overdue_90,
    sm.broken_signatures,
    sm.crfs_never_signed,
    
    -- Subject Status
    sm.subject_status,
    sm.latest_visit,
    
    -- Data Quality Index (0-100)
    CAST(
        (CASE WHEN sm.expected_visits > 0 THEN (1 - CAST(sm.missing_visits AS FLOAT)/sm.expected_visits) * 15 ELSE 15 END) +
        (COALESCE(sm.percent_clean_crf, 0) * 0.20) +
        (CASE WHEN sm.total_queries > 0 THEN 0 ELSE 15 END) +
        (CASE WHEN (sm.coded_terms + sm.uncoded_terms) > 0 
            THEN (CAST(sm.coded_terms AS FLOAT)/(sm.coded_terms + sm.uncoded_terms)) * 15 
            ELSE 15 END) +
        (CASE WHEN sm.crfs_require_verification > 0 
            THEN (CAST(sm.forms_verified AS FLOAT)/sm.crfs_require_verification) * 15 
            ELSE 15 END) +
        (CASE WHEN (sm.open_issues_lnr + sm.open_issues_edrr + sm.esae_review_dm + sm.esae_review_safety) = 0 THEN 20 ELSE 0 END)
    AS DECIMAL(5,2)) AS data_quality_index,
    
    -- Clean Patient Flag
    CASE 
        WHEN sm.missing_visits = 0 
            AND sm.total_queries = 0 
            AND sm.missing_pages = 0
            AND sm.pages_non_conformant = 0
            AND sm.uncoded_terms = 0
            AND (sm.crfs_require_verification = 0 OR sm.crfs_require_verification = sm.forms_verified)
        THEN 1 
        ELSE 0 
    END AS is_clean_patient,
    
    sm.dwh_create_date AS snapshot_date

FROM silver.cpid_edc_subject_metrics sm;
GO

-- =============================================================================
-- Fact Table: gold.fact_query_metrics
-- =============================================================================
IF OBJECT_ID('gold.fact_query_metrics', 'V') IS NOT NULL
    DROP VIEW gold.fact_query_metrics;
GO

CREATE VIEW gold.fact_query_metrics AS
SELECT
    q.study_id,
    q.site_id,
    q.subject_id,
    q.region,
    q.country,
    q.folder_name,
    q.form_name,
    q.field_oid,
    q.log_no,
    q.visit_date,
    q.query_status,
    q.action_owner,
    q.marking_group_name,
    q.query_open_date,
    q.query_response,
    q.days_since_open,
    q.days_since_response,
    CASE WHEN q.query_status = 'Open' THEN 1 ELSE 0 END AS is_open,
    CASE WHEN q.query_status = 'Answered' THEN 1 ELSE 0 END AS is_answered,
    CASE WHEN q.query_status = 'Closed' THEN 1 ELSE 0 END AS is_closed,
    CASE 
        WHEN q.days_since_open <= 7 THEN '0-7 Days'
        WHEN q.days_since_open <= 14 THEN '8-14 Days'
        WHEN q.days_since_open <= 30 THEN '15-30 Days'
        WHEN q.days_since_open <= 60 THEN '31-60 Days'
        ELSE '60+ Days'
    END AS query_age_bucket
FROM silver.cpid_edc_query_report_cumulative q;
GO

-- =============================================================================
-- Fact Table: gold.fact_sdv_status
-- =============================================================================
IF OBJECT_ID('gold.fact_sdv_status', 'V') IS NOT NULL
    DROP VIEW gold.fact_sdv_status;
GO

CREATE VIEW gold.fact_sdv_status AS
SELECT
    sdv.study_id,
    sdv.site_id,
    sdv.subject_id,
    sdv.region,
    sdv.country,
    sdv.folder_name,
    sdv.data_page_name,
    sdv.visit_date,
    sdv.verification_status,
    CASE WHEN sdv.verification_status = 'Verifed' THEN 1 ELSE 0 END AS is_verified,
    CASE WHEN sdv.verification_status != 'Verifed' THEN 1 ELSE 0 END AS is_not_verified
FROM silver.cpid_edc_sdv sdv;
GO

-- =============================================================================
-- Fact Table: gold.fact_missing_visits
-- =============================================================================
IF OBJECT_ID('gold.fact_missing_visits', 'V') IS NOT NULL
    DROP VIEW gold.fact_missing_visits;
GO

CREATE VIEW gold.fact_missing_visits AS
SELECT
    vpt.study_id,
    vpt.site_id,
    vpt.subject_id,
    vpt.country,
    vpt.visit AS visit_name,
    vpt.projected_date,
    TRY_CAST(vpt.days_outstanding AS INT) AS days_outstanding,
    CASE 
        WHEN TRY_CAST(vpt.days_outstanding AS INT) <= 0 THEN 'On Track'
        WHEN TRY_CAST(vpt.days_outstanding AS INT) <= 7 THEN '1-7 Days Overdue'
        WHEN TRY_CAST(vpt.days_outstanding AS INT) <= 14 THEN '8-14 Days Overdue'
        WHEN TRY_CAST(vpt.days_outstanding AS INT) <= 30 THEN '15-30 Days Overdue'
        ELSE '30+ Days Overdue'
    END AS overdue_bucket,
    CASE WHEN TRY_CAST(vpt.days_outstanding AS INT) > 0 THEN 1 ELSE 0 END AS is_overdue
FROM silver.visit_projection_tracker vpt;
GO

-- =============================================================================
-- Fact Table: gold.fact_missing_pages
-- =============================================================================
IF OBJECT_ID('gold.fact_missing_pages', 'V') IS NOT NULL
    DROP VIEW gold.fact_missing_pages;
GO

CREATE VIEW gold.fact_missing_pages AS
SELECT
    mp.study_id,
    mp.site_id,
    mp.subject_id,
    mp.site_group AS country,
    mp.folder_name,
    mp.form_name,
    mp.visit_date,
    mp.overall_subject_status,
    mp.visit_level_subject_status,
    mp.days_page_missing,
    CASE 
        WHEN mp.days_page_missing <= 7 THEN '0-7 Days'
        WHEN mp.days_page_missing <= 14 THEN '8-14 Days'
        WHEN mp.days_page_missing <= 30 THEN '15-30 Days'
        WHEN mp.days_page_missing <= 60 THEN '31-60 Days'
        ELSE '60+ Days'
    END AS missing_duration_bucket
FROM silver.missing_pages_visit_level mp;
GO

-- =============================================================================
-- Fact Table: gold.fact_non_conformant
-- =============================================================================
IF OBJECT_ID('gold.fact_non_conformant', 'V') IS NOT NULL
    DROP VIEW gold.fact_non_conformant;
GO

CREATE VIEW  gold.fact_non_conformant AS
SELECT
    nc.study_id,
    nc.site_id,
    nc.subject_id,
    nc.region,
    nc.country,
    nc.folder_name,
    nc.page_name,
    nc.log_no,
    nc.field_oid,
    nc.audit_time,
    nc.visit_date,
    1 AS non_conformant_count
FROM silver.cpid_edc_non_conformant nc;
GO

-- =============================================================================
-- Fact Table: gold.fact_signature_status
-- =============================================================================
IF OBJECT_ID('gold.fact_signature_status', 'V') IS NOT NULL
    DROP VIEW gold.fact_signature_status;
GO

CREATE VIEW    gold.fact_signature_status AS
SELECT
    ps.study_id,
    ps.site_id,
    ps.subject_id,
    ps.region,
    ps.country,
    ps.visit_name,
    ps.form_name,
    ps.visit_date,
    ps.page_require_signature,
    ps.audit_action,
    ps.date_last_pi_sign,
    ps.no_of_days AS days_since_entry,
    ps.pending_since_pi_signed,
    0 AS is_signed,
    CASE WHEN ps.pending_since_pi_signed IS NOT NULL THEN 1 ELSE 0 END AS is_pending,
    CASE 
        WHEN ps.no_of_days <= 45 THEN 'Within 45 Days'
        WHEN ps.no_of_days <= 90 THEN '45-90 Days Overdue'
        ELSE 'Over 90 Days Overdue'
    END AS signature_aging_bucket
FROM silver.cpid_edc_pi_signature_report ps;
GO

-- =============================================================================
-- Fact Table: gold.fact_sae_dashboard
-- =============================================================================
IF OBJECT_ID('gold.fact_sae_dashboard', 'V') IS NOT NULL
    DROP VIEW gold.fact_sae_dashboard;
GO

CREATE VIEW gold.fact_sae_dashboard AS
SELECT
    dm.study_id,
    dm.site_id,
    dm.patient_id AS subject_id,
    dm.country,
    dm.discrepancy_id,
    dm.form_name,
    dm.discrepancy_ts,
    dm.review_status,
    dm.action_status,
    'Data Management' AS review_type,
    CASE WHEN dm.review_status = 'Review Completed' THEN 1 ELSE 0 END AS is_review_complete,
    CASE WHEN dm.action_status = 'No action required' THEN 1 ELSE 0 END AS is_action_complete
FROM silver.sae_dashboard_dm dm
UNION ALL
SELECT
    sf.study_id,
    sf.site_id,
    sf.patient_id AS subject_id,
    NULL AS country,
    sf.discrepancy_id,
    NULL AS form_name,
    sf.discrepancy_ts,
    sf.review_status,
    sf.action_status,
    'Safety' AS review_type,
    CASE WHEN sf.review_status = 'Review Completed' THEN 1 ELSE 0 END AS is_review_complete,
    CASE WHEN sf.action_status = 'No action required' THEN 1 ELSE 0 END AS is_action_complete
FROM silver.sae_dashboard_safety sf;
GO

-- =============================================================================
-- Fact Table: gold.fact_coding_status
-- =============================================================================
IF OBJECT_ID('gold.fact_coding_status', 'V') IS NOT NULL
    DROP VIEW gold.fact_coding_status;
GO

CREATE VIEW gold.fact_coding_status AS
SELECT
    md.study_id,
    md.subject_id,
    md.dictionary,
    md.dictionary_version,
    md.form_oid,
    md.logline,
    md.field_oid,
    md.coding_status,
    md.require_coding,
    'MedDRA' AS coding_type,
    CASE WHEN md.coding_status = 'Coded Term' THEN 1 ELSE 0 END AS is_coded,
    CASE WHEN md.coding_status = 'UnCoded Term' THEN 1 ELSE 0 END AS is_uncoded,
    CASE WHEN md.require_coding = 'Yes' THEN 1 ELSE 0 END AS requires_coding
FROM silver.globalcodingreport_meddra md
UNION ALL
SELECT
    wd.study_id,
    wd.subject_id,
    wd.dictionary,
    wd.dictionary_version,
    wd.form_oid,
    wd.logline,
    wd.field_oid,
    wd.coding_status,
    wd.require_coding,
    'WHODrug' AS coding_type,
    CASE WHEN wd.coding_status = 'Coded Term' THEN 1 ELSE 0 END AS is_coded,
    CASE WHEN wd.coding_status = 'UnCoded Term' THEN 1 ELSE 0 END AS is_uncoded,
    CASE WHEN wd.require_coding = 'Yes' THEN 1 ELSE 0 END AS requires_coding
FROM silver.globalcodingreport_whodra wd;
GO

-- =============================================================================
-- Fact Table: gold.fact_lab_issues
-- =============================================================================
IF OBJECT_ID('gold.fact_lab_issues', 'V') IS NOT NULL
    DROP VIEW gold.fact_lab_issues;
GO

CREATE VIEW  gold.fact_lab_issues AS
SELECT
    lr.study_id,
    lr.site_id,
    lr.subject_id,
    lr.country,
    lr.visit,
    lr.form_name,
    lr.lab_category,
    lr.lab_date,
    lr.test_name,
    lr.test_description,
    lr.issue,
    lr.comments,
    CASE WHEN lr.issue LIKE '%Missing Lab%' THEN 1 ELSE 0 END AS is_missing_lab_name,
    CASE WHEN lr.issue LIKE '%Ranges%' OR lr.issue LIKE '%Units%' THEN 1 ELSE 0 END AS is_missing_ranges,
    1 AS issue_count
FROM silver.missing_lab_ranges lr;
GO

-- =============================================================================
-- Fact Table: gold.fact_inactivated_records
-- =============================================================================
IF OBJECT_ID('gold.fact_inactivated_records', 'V') IS NOT NULL
    DROP VIEW gold.fact_inactivated_records;
GO

CREATE VIEW  gold.fact_inactivated_records AS
SELECT
    ir.study_id,
    ir.site_id,
    ir.subject_id,
    ir.country,
    ir.folder,
    ir.form,
    ir.data_on_form,
    ir.record_position,
    ir.audit_action,
    CASE WHEN ir.data_on_form = 'Y' THEN 1 ELSE 0 END AS has_data,
    1 AS inactivation_count
FROM silver.inactivated_forms_loglines ir;
GO

-- =============================================================================
-- Fact Table: gold.fact_protocol_deviations
-- =============================================================================
IF OBJECT_ID('gold.fact_protocol_deviations', 'V') IS NOT NULL
    DROP VIEW gold.fact_protocol_deviations;
GO

CREATE VIEW  gold.fact_protocol_deviations AS
SELECT
    pd.study_id,
    pd.site_id,
    pd.subject_id,
    pd.region,
    pd.country,
    pd.folder_name,
    pd.form_name,
    pd.log_no,
    pd.pd_status,
    pd.visit_date,
    CASE WHEN pd.pd_status = 'PD Confirmed' THEN 1 ELSE 0 END AS is_confirmed,
    CASE WHEN pd.pd_status = 'PD Proposed' THEN 1 ELSE 0 END AS is_proposed,
    1 AS deviation_count
FROM silver.cpid_edc_query_protocol_deviation pd;
GO

-- =============================================================================
-- Fact Table: gold.fact_crf_lock_freeze
-- =============================================================================
IF OBJECT_ID('gold.fact_crf_lock_freeze', 'V') IS NOT NULL
    DROP VIEW gold.fact_crf_lock_freeze;
GO

CREATE VIEW   gold.fact_crf_lock_freeze AS
SELECT study_id, site_id, subject_id, region, country, page_name, visit_date,
    lock_status AS status, 'Lock' AS status_type,
    CASE WHEN lock_status = 'Lock' THEN 1 ELSE 0 END AS is_locked,
    0 AS is_unlocked, 0 AS is_frozen, 0 AS is_unfrozen
FROM  silver.cpid_edc_crf_locked
UNION ALL
SELECT study_id, site_id, subject_id, region, country, page_name, visit_date,
    lock_unlock_status AS status, 'Unlock' AS status_type,
    0 AS is_locked,
    CASE WHEN lock_unlock_status = 'Unlock' THEN 1 ELSE 0 END AS is_unlocked,
    0 AS is_frozen, 0 AS is_unfrozen
FROM silver.cpid_edc_crf_unlocked
UNION ALL
SELECT study_id, site_id, subject_id, region, country, page_name, visit_date,
    freeze_status AS status, 'Freeze' AS status_type,
    0 AS is_locked, 0 AS is_unlocked,
    CASE WHEN freeze_status = 'Freeze' THEN 1 ELSE 0 END AS is_frozen,
    0 AS is_unfrozen
FROM  silver.cpid_edc_crf_freeze
UNION ALL
SELECT study_id, site_id, subject_id, region, country, page_name, visit_date,
    unfreeze_status AS status, 'Unfreeze' AS status_type,
    0 AS is_locked, 0 AS is_unlocked, 0 AS is_frozen,
    CASE WHEN unfreeze_status = 'UnFreeze' THEN 1 ELSE 0 END AS is_unfrozen
FROM silver.cpid_edc_crf_unfreeze;
GO

-- =============================================================================
-- Fact Table: gold.fact_edrr_issues
-- =============================================================================
IF OBJECT_ID('gold.fact_edrr_issues', 'V') IS NOT NULL
    DROP VIEW gold.fact_edrr_issues;
GO

CREATE VIEW gold.fact_edrr_issues AS
SELECT
    er.study_id,
    er.subject_id,
    er.total_open_issue_count_per_subject AS open_issue_count,
    CASE 
        WHEN er.total_open_issue_count_per_subject = 0 THEN 'No Issues'
        WHEN er.total_open_issue_count_per_subject <= 5 THEN 'Low (1-5)'
        WHEN er.total_open_issue_count_per_subject <= 10 THEN 'Medium (6-10)'
        ELSE 'High (10+)'
    END AS issue_severity_bucket
FROM silver.compiled_edrr er;
GO

-- =============================================================================
-- AGGREGATE VIEWS
-- =============================================================================

-- =============================================================================
-- Aggregate View: gold.agg_site_performance
-- =============================================================================
IF OBJECT_ID('gold.agg_site_performance', 'V') IS NOT NULL
    DROP VIEW gold.agg_site_performance;
GO

CREATE VIEW gold.agg_site_performance AS
SELECT
    fsm.study_id, fsm.site_id, fsm.country, fsm.region,
    COUNT(DISTINCT fsm.subject_id) AS total_subjects,
    SUM(fsm.is_clean_patient) AS clean_subjects,
    CAST(SUM(fsm.is_clean_patient) AS FLOAT) / NULLIF(COUNT(DISTINCT fsm.subject_id), 0) * 100 AS pct_clean_subjects,
    SUM(fsm.missing_visits) AS total_missing_visits,
    SUM(fsm.total_queries) AS total_open_queries,
    AVG(fsm.percent_clean_crf) AS avg_pct_clean_crf,
    AVG(fsm.data_quality_index) AS avg_data_quality_index,
    SUM(fsm.crfs_require_sdv) AS total_crfs_require_sdv,
    SUM(fsm.forms_verified) AS total_forms_verified,
    CAST(SUM(fsm.forms_verified) AS FLOAT) / NULLIF(SUM(fsm.crfs_require_sdv), 0) * 100 AS pct_sdv_complete,
    SUM(fsm.pds_confirmed) AS total_pds_confirmed,
    SUM(fsm.crfs_overdue_90) AS total_crfs_overdue_90_days
FROM gold.fact_subject_metrics fsm
GROUP BY fsm.study_id, fsm.site_id, fsm.country, fsm.region;
GO

-- =============================================================================
-- Aggregate View: gold.agg_country_performance
-- =============================================================================
IF OBJECT_ID('gold.agg_country_performance', 'V') IS NOT NULL
    DROP VIEW gold.agg_country_performance;
GO

CREATE VIEW gold.agg_country_performance AS
SELECT
    fsm.study_id, fsm.country, fsm.region,
    COUNT(DISTINCT fsm.site_id) AS total_sites,
    COUNT(DISTINCT fsm.subject_id) AS total_subjects,
    SUM(fsm.is_clean_patient) AS clean_subjects,
    CAST(SUM(fsm.is_clean_patient) AS FLOAT) / NULLIF(COUNT(DISTINCT fsm.subject_id), 0) * 100 AS pct_clean_subjects,
    AVG(fsm.data_quality_index) AS avg_data_quality_index,
    SUM(fsm.total_queries) AS total_open_queries,
    SUM(fsm.missing_visits) AS total_missing_visits,
    SUM(fsm.pds_confirmed) AS total_pds_confirmed
FROM gold.fact_subject_metrics fsm
GROUP BY fsm.study_id, fsm.country, fsm.region;
GO

-- =============================================================================
-- Aggregate View: gold.agg_study_summary
-- =============================================================================
IF OBJECT_ID('gold.agg_study_summary', 'V') IS NOT NULL
    DROP VIEW gold.agg_study_summary;
GO

CREATE VIEW  gold.agg_study_summary AS
SELECT
    fsm.study_id,
    COUNT(DISTINCT fsm.region) AS total_regions,
    COUNT(DISTINCT fsm.country) AS total_countries,
    COUNT(DISTINCT fsm.site_id) AS total_sites,
    COUNT(DISTINCT fsm.subject_id) AS total_subjects,
    SUM(fsm.is_clean_patient) AS clean_subjects,
    CAST(SUM(fsm.is_clean_patient) AS FLOAT) / NULLIF(COUNT(DISTINCT fsm.subject_id), 0) * 100 AS pct_clean_subjects,
    AVG(fsm.data_quality_index) AS avg_data_quality_index,
    SUM(fsm.total_queries) AS total_open_queries,
    SUM(fsm.missing_visits) AS total_missing_visits,
    SUM(fsm.missing_pages) AS total_missing_pages,
    CAST(SUM(fsm.forms_verified) AS FLOAT) / NULLIF(SUM(fsm.crfs_require_sdv), 0) * 100 AS overall_pct_sdv_complete,
    SUM(fsm.pds_confirmed) AS total_pds,
    SUM(fsm.uncoded_terms) AS total_uncoded_terms,
    SUM(fsm.crfs_overdue_90) AS total_signature_overdue_90,
    CASE 
        WHEN AVG(fsm.data_quality_index) >= 90 AND SUM(fsm.total_queries) = 0 AND SUM(fsm.uncoded_terms) = 0 THEN 'Ready'
        WHEN AVG(fsm.data_quality_index) >= 75 THEN 'Near Ready'
        ELSE 'Not Ready'
    END AS submission_readiness
FROM gold.fact_subject_metrics fsm
GROUP BY fsm.study_id;
GO

-- =============================================================================
-- View: gold.vw_action_items
-- =============================================================================
IF OBJECT_ID('gold.vw_action_items', 'V') IS NOT NULL
    DROP VIEW gold.vw_action_items;
GO

CREATE VIEW  select * from  gold.vw_action_items AS
SELECT 'CRITICAL' AS priority, 'Signature Overdue >90 Days' AS action_type,
    study_id, site_id, subject_id, region, country,
    CONCAT(CAST(crfs_overdue_90 AS VARCHAR), ' CRFs overdue') AS action_description,
    'Investigator' AS responsible_party
FROM gold.fact_subject_metrics WHERE crfs_overdue_90 > 0
UNION ALL
SELECT 'HIGH', 'Safety Query Resolution',
    study_id, site_id, subject_id, region, country,
    CONCAT(CAST(safety_queries AS VARCHAR), ' safety queries'), 'Safety Team'
FROM gold.fact_subject_metrics WHERE safety_queries > 0
UNION ALL
SELECT 'MEDIUM', 'Missing Visit Follow-up',
    study_id, site_id, subject_id, region, country,
    CONCAT(CAST(missing_visits AS VARCHAR), ' visits missing'), 'Site/CRA'
FROM gold.fact_subject_metrics WHERE missing_visits > 0
UNION ALL
SELECT 'MEDIUM', 'Coding Required',
    study_id, site_id, subject_id, region, country,
    CONCAT(CAST(uncoded_terms AS VARCHAR), ' terms need coding'), 'Coder'
FROM gold.fact_subject_metrics WHERE uncoded_terms > 0;
GO
