
IF OBJECT_ID('silver.cpid_edc_sdv', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_sdv;
GO

CREATE TABLE silver.cpid_edc_sdv (
    study_id            NVARCHAR(50),
    region              NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    folder_name         NVARCHAR(255),
    data_page_name      NVARCHAR(50), 
    visit_date          DATE,
    verification_status NVARCHAR(50),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 2. cpid_edc_crf_unlocked
IF OBJECT_ID('silver.cpid_edc_crf_unlocked', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_crf_unlocked;
GO

CREATE TABLE silver.cpid_edc_crf_unlocked (
    study_id            NVARCHAR(50),
    region              NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    page_name           NVARCHAR(50),
    lock_unlock_status  NVARCHAR(50),
    visit_date          DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 3. cpid_edc_crf_freeze
IF OBJECT_ID('silver.cpid_edc_crf_freeze', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_crf_freeze;
GO

CREATE TABLE silver.cpid_edc_crf_freeze (
    study_id            NVARCHAR(50),
    region              NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    page_name           NVARCHAR(50),
    freeze_status       NVARCHAR(50),
    visit_date          DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 4. cpid_edc_crf_locked
IF OBJECT_ID('silver.cpid_edc_crf_locked', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_crf_locked;
GO

CREATE TABLE silver.cpid_edc_crf_locked (
    study_id            NVARCHAR(50),
    region              NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    page_name           NVARCHAR(50),
    lock_status         NVARCHAR(50),
    visit_date          DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 5. cpid_edc_crf_unfreeze
IF OBJECT_ID('silver.cpid_edc_crf_unfreeze', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_crf_unfreeze;
GO

CREATE TABLE silver.cpid_edc_crf_unfreeze (
    study_id            NVARCHAR(50),
    region              NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    page_name           NVARCHAR(50),
    unfreeze_status     NVARCHAR(50),
    visit_date          DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 6. cpid_edc_pi_signature_report
IF OBJECT_ID('silver.cpid_edc_pi_signature_report', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_pi_signature_report;
go
CREATE TABLE silver.cpid_edc_pi_signature_report (
    study_id                    NVARCHAR(50),
    region                      NVARCHAR(50),
    country                     NVARCHAR(50),
    site_id                     NVARCHAR(50),
    subject_id                  NVARCHAR(50),
    visit_name                  NVARCHAR(255),
    form_name                   NVARCHAR(50),
    page_require_signature      NVARCHAR(50),
    audit_action                NVARCHAR(50),
    visit_date                  DATE,
    date_last_pi_sign           DATE,
    no_of_days                  INT,
    pending_since_pi_signed     NVARCHAR(50),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- 7. cpid_edc_non_conformant
IF OBJECT_ID('silver.cpid_edc_non_conformant', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_non_conformant;
GO

CREATE TABLE silver.cpid_edc_non_conformant (
    study_id            NVARCHAR(50),
    region              NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    folder_name         NVARCHAR(50),
    page_name           NVARCHAR(50),
    log_no              INT,
    field_oid           NVARCHAR(50),
    audit_time          DATETIME,
    visit_date          DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 8. cpid_edc_query_report_cra_action
IF OBJECT_ID('silver.cpid_edc_query_report_cra_action', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_query_report_cra_action;
GO

CREATE TABLE silver.cpid_edc_query_report_cra_action (
    study_id                NVARCHAR(50),
    region                  NVARCHAR(50),
    country                 NVARCHAR(50),
    site_id                 NVARCHAR(50),
    subject_id              NVARCHAR(50),
    folder_name             NVARCHAR(50),
    form_name               NVARCHAR(50),
    field_oid               NVARCHAR(50),
    log_no                  INT,
    visit_date              DATE,
    query_status            NVARCHAR(50),
    action_owner            NVARCHAR(50),
    marking_group_name      NVARCHAR(50),
    query_open_date         DATE,
    query_response          NVARCHAR(50),
    days_since_open         INT,
    days_since_response     INT,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- 9. cpid_edc_query_protocol_deviation
IF OBJECT_ID('silver.cpid_edc_query_protocol_deviation', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_query_protocol_deviation;
GO

CREATE TABLE silver.cpid_edc_query_protocol_deviation (
    study_id        NVARCHAR(50),
    region          NVARCHAR(50),
    country         NVARCHAR(50),
    site_id         NVARCHAR(50),
    subject_id      NVARCHAR(50),
    folder_name     NVARCHAR(100),
    form_name       NVARCHAR(50),
    log_no          INT,
    pd_status       NVARCHAR(50),
    visit_date      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- 10. cpid_edc_query_report_site_action
IF OBJECT_ID('silver.cpid_edc_query_report_site_action', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_query_report_site_action;
GO

CREATE TABLE silver.cpid_edc_query_report_site_action (
    study_id                NVARCHAR(50),
    region                  NVARCHAR(50),
    country                 NVARCHAR(50),
    site_id                 NVARCHAR(50),
    subject_id              NVARCHAR(50),
    folder_name             NVARCHAR(150),
    form_name               NVARCHAR(50),
    field_oid               NVARCHAR(50),
    log_no                  INT,
    visit_date              DATE,
    query_status            NVARCHAR(50),
    action_owner            NVARCHAR(50),
    marking_group_name      NVARCHAR(50),
    query_open_date         DATE,
    query_response          NVARCHAR(50),
    days_since_open         INT,
    days_since_response     INT,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- 11. cpid_edc_query_report_cumulative
IF OBJECT_ID('silver.cpid_edc_query_report_cumulative', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_query_report_cumulative;
GO

CREATE TABLE silver.cpid_edc_query_report_cumulative (
    study_id                NVARCHAR(50),
    region                  NVARCHAR(50),
    country                 NVARCHAR(50),
    site_id                 NVARCHAR(50),
    subject_id              NVARCHAR(50),
    folder_name             NVARCHAR(150),
    form_name               NVARCHAR(50),
    field_oid               NVARCHAR(50),
    log_no                  INT,
    visit_date              DATE,
    query_status            NVARCHAR(50),
    action_owner            NVARCHAR(50),
    marking_group_name      NVARCHAR(50),
    query_open_date         DATE,
    query_response          date,
    days_since_open         INT,
    days_since_response     INT,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- 12. cpid_edc_sv
IF OBJECT_ID('silver.cpid_edc_sv', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_sv;
GO

CREATE TABLE silver.cpid_edc_sv (
    project_name    NVARCHAR(50),
    region          NVARCHAR(50),
    country         NVARCHAR(50),
    site_id         NVARCHAR(50),
    subject_name    NVARCHAR(50),
    folder_name     NVARCHAR(50),
    visit_date      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.compiled_edrr', 'U') IS NOT NULL
    DROP TABLE silver.compiled_edrr;
GO

CREATE TABLE silver.compiled_edrr (
    study_id            NVARCHAR(50),
    subject_id      NVARCHAR(50),
    total_open_issue_count_per_subject       INT,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.globalcodingreport_meddra', 'U') IS NOT NULL
    DROP TABLE silver.globalcodingreport_meddra;
GO
CREATE TABLE silver.globalcodingreport_meddra (
    study_id            NVARCHAR(50),
    dictionary          NVARCHAR(50),
    dictionary_version  NVARCHAR(50),
    subject_id          NVARCHAR(50),
    form_oid            NVARCHAR(50),
    logline             INT,
    field_oid           NVARCHAR(50),
    coding_status       NVARCHAR(50),
    require_coding      NVARCHAR(50),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.globalcodingreport_whodra', 'U') IS NOT NULL
    DROP TABLE silver.globalcodingreport_whodra;
GO
CREATE TABLE silver.globalcodingreport_whodra (
    study_id            NVARCHAR(50),
    dictionary          NVARCHAR(50),
    dictionary_version  NVARCHAR(50),
    subject_id          NVARCHAR(50),
    form_oid            NVARCHAR(50),
    logline             INT,
    field_oid           NVARCHAR(50),
    coding_status       NVARCHAR(50),
    require_coding      NVARCHAR(50),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.missing_pages_visit_level', 'U') IS NOT NULL
    DROP TABLE silver.missing_pages_visit_level;
GO
CREATE TABLE silver.missing_pages_visit_level (
    study_id                     NVARCHAR(50),
    site_group                   NVARCHAR(50),
    site_id                      NVARCHAR(50),
    subject_id                   NVARCHAR(50),

    overall_subject_status        NVARCHAR(50),
    visit_level_subject_status    NVARCHAR(100),
    form_subject_status           NVARCHAR(50),

    visit_name                    NVARCHAR(50),
    folder_name                   NVARCHAR(50),
    form_name                     NVARCHAR(50),

    visit_date                date, --SELECT CONVERT(date, '15 SEP 2025', 106);

    days_page_missing             INT,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
GO
IF OBJECT_ID('silver.missing_pages_all', 'U') IS NOT NULL
    DROP TABLE silver.missing_pages_all;
GO
CREATE TABLE silver.missing_pages_all (
    study_id                     NVARCHAR(50),
    site_group                   NVARCHAR(50),
    site_id                      NVARCHAR(50),
    subject_id                   NVARCHAR(50),

    overall_subject_status        NVARCHAR(50),
    visit_level_subject_status    NVARCHAR(100),   -- nullable (not always present)

    folder_name                   NVARCHAR(50),
    page_name                     NVARCHAR(50),

    visit_date                DATE,  --SELECT CONVERT(date, '15 SEP 2025', 106); 

    days_page_missing             INT,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.inactivated_forms_loglines', 'U') IS NOT NULL
    DROP TABLE silver.inactivated_forms_loglines;
GO
CREATE TABLE silver.inactivated_forms_loglines (
	study_id                NVARCHAR(50),
    country                 NVARCHAR(50),
    site_id               NVARCHAR(50),
    subject_id              NVARCHAR(50),
    folder            NVARCHAR(100),
    form               NVARCHAR(50),
    data_on_form      CHAR(5),      -- Y / N
    record_position         INT,
    audit_action            VARCHAR(100),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.sae_dashboard_safety', 'U') IS NOT NULL
    DROP TABLE silver.sae_dashboard_safety ;
GO
CREATE TABLE silver.sae_dashboard_safety (
    discrepancy_id      NVARCHAR(50),
    study_id           NVARCHAR(50),
    site_id             NVARCHAR(50),
    patient_id          NVARCHAR(50),
    case_status         NVARCHAR(50),
    discrepancy_ts      DATE,
    review_status       NVARCHAR(50),
    action_status       NVARCHAR(50),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.sae_dashboard_dm', 'U') IS NOT NULL
    DROP TABLE silver.sae_dashboard_dm;
GO
CREATE TABLE silver.sae_dashboard_dm (
    discrepancy_id      NVARCHAR(50),
    study_id            NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    patient_id          NVARCHAR(50),
    form_name           NVARCHAR(50),
    discrepancy_ts      DATE,
    review_status       NVARCHAR(50),
    action_status       NVARCHAR(100),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.missing_lab_ranges', 'U') IS NOT NULL
    DROP TABLE silver.missing_lab_ranges;
GO
CREATE TABLE silver.missing_lab_ranges (
	study_id            NVARCHAR(50),
    country             NVARCHAR(50),
    site_id             NVARCHAR(50),
    subject_id          NVARCHAR(50),
    visit          NVARCHAR(50),
    form_name           NVARCHAR(50),
    lab_category        NVARCHAR(50),
    lab_date            DATE,
    test_name           NVARCHAR(50),
    test_description    NVARCHAR(50),
    issue               NVARCHAR(50),
    comments            VARCHAR(55),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.visit_projection_tracker', 'U') IS NOT NULL
    DROP TABLE silver.visit_projection_tracker;
GO
CREATE TABLE silver.visit_projection_tracker (
	study_id            NVARCHAR(50),
    country             NVARCHAR(50),
    site_id            NVARCHAR(50),
    subject_id          NVARCHAR(50),
    visit          NVARCHAR(50),
    projected_date      DATE,
    days_outstanding    VARCHAR(50),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('silver.cpid_edc_subject_metrics', 'U') IS NOT NULL
    DROP TABLE silver.cpid_edc_subject_metrics;
GO
CREATE TABLE silver.cpid_edc_subject_metrics (
    study_id                NVARCHAR(50),
    region                      NVARCHAR(50),
    country                     NVARCHAR(50),
    site_id                     NVARCHAR(50),
    subject_id                  NVARCHAR(50),
    latest_visit                NVARCHAR(50),
    subject_status              NVARCHAR(50),
    missing_visits              INT,
    missing_pages               INT,
    coded_terms                 INT,
    uncoded_terms               INT,
    open_issues_lnr             INT,
    open_issues_edrr            INT,
    inactivated_forms           INT,
    esae_review_dm              INT,
    esae_review_safety          INT,
    expected_visits             INT,
    pages_entered               INT,
    pages_non_conformant        INT,
    crfs_with_issues            INT,
    crfs_clean                  INT,
    percent_clean_crf           float,
    dm_queries                  INT,
    clinical_queries            INT,
    medical_queries             INT,
    site_queries                INT,
    field_monitor_queries       INT,
    coding_queries              INT,
    safety_queries              INT,
    total_queries               INT,
    crfs_require_verification   INT,
    forms_verified              INT,
    crfs_frozen                 INT,
    crfs_not_frozen             INT,
    crfs_locked                 INT,
    crfs_unlocked               INT,
    pds_confirmed               INT,
    pds_proposed                INT,
    crfs_signed                 INT,
    crfs_overdue_45            INT,
    crfs_overdue_45_90           INT,
    crfs_overdue_90             INT,
    broken_signatures           INT,
    crfs_never_signed           INT,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
