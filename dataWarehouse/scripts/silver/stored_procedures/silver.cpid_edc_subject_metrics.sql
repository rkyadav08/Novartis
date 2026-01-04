CREATE OR ALTER PROCEDURE silver.load_cpid_edc_subject_metrics
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
        PRINT 'Dataset: CPID EDC Subject Metrics';
        PRINT '================================================';

        PRINT '>> Truncating Table: silver.cpid_edc_subject_metrics';
        TRUNCATE TABLE silver.cpid_edc_subject_metrics;

        SET @start_time = GETDATE();

        PRINT '>> Inserting cleaned data with calculated fields into silver.cpid_edc_subject_metrics';

        -- Create normalized IDs for matching
        WITH normalized_base AS (
            SELECT 
                *,
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id
            FROM bronze.cpid_edc_subject_metrics
            WHERE study_id IS NOT NULL 
              AND subject_id IS NOT NULL
              AND UPPER(LTRIM(study_id)) LIKE 'STUDY%'
        ),
        -- Missing Visits
        mv_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                COUNT(DISTINCT visit_name) AS missing_visit_count
            FROM silver.missing_pages_visit_level
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- Missing Pages
        mp_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                COUNT(*) AS missing_page_count
            FROM silver.missing_pages_all
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- Coded Terms
        coded_calc AS (
            SELECT 
                norm_study_id,
                norm_subject_id,
                COUNT(*) AS coded_count
            FROM (
                SELECT 
                    UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                    CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                    END AS norm_subject_id
                FROM silver.globalcodingreport_meddra 
                WHERE UPPER(LTRIM(RTRIM(coding_status))) LIKE '%CODED%'
                  AND UPPER(LTRIM(RTRIM(coding_status))) NOT LIKE '%UNCODED%'
                UNION ALL
                SELECT 
                    UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                    CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                    END AS norm_subject_id
                FROM silver.globalcodingreport_whodra 
                WHERE UPPER(LTRIM(RTRIM(coding_status))) LIKE '%CODED%'
                  AND UPPER(LTRIM(RTRIM(coding_status))) NOT LIKE '%UNCODED%'
            ) coded
            GROUP BY norm_study_id, norm_subject_id
        ),
        -- Uncoded Terms
        uncoded_calc AS (
            SELECT 
                norm_study_id,
                norm_subject_id,
                COUNT(*) AS uncoded_count
            FROM (
                SELECT 
                    UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                    CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                    END AS norm_subject_id
                FROM silver.globalcodingreport_meddra 
                WHERE UPPER(LTRIM(RTRIM(ISNULL(coding_status, '')))) LIKE '%UNCODED%'
                UNION ALL
                SELECT 
                    UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                    CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                    END AS norm_subject_id
                FROM silver.globalcodingreport_whodra 
                WHERE UPPER(LTRIM(RTRIM(ISNULL(coding_status, '')))) LIKE '%UNCODED%'
            ) uncoded
            GROUP BY norm_study_id, norm_subject_id
        ),
        -- Open Issues in LNR (Lab and Ranges) - FIXED!
        lnr_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                COUNT(*) AS open_lnr_count
            FROM silver.missing_lab_ranges
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- EDRR Issues
        edrr_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                MAX(total_open_issue_count_per_subject) AS total_open_issue_count_per_subject
            FROM silver.compiled_edrr
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- Inactivated Forms
        inact_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                COUNT(DISTINCT folder + '|' + form) AS inactivated_count
            FROM silver.inactivated_forms_loglines
            WHERE UPPER(LTRIM(RTRIM(audit_action))) LIKE '%INACTIVAT%'
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- SAE DM
        sae_dm_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN patient_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(patient_id, 9, LEN(patient_id)))))
                    ELSE UPPER(LTRIM(RTRIM(patient_id)))
                END AS norm_subject_id,
                COUNT(*) AS pending_dm_count
            FROM silver.sae_dashboard_dm
            WHERE UPPER(LTRIM(RTRIM(ISNULL(review_status, '')))) != 'COMPLETED'
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN patient_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(patient_id, 9, LEN(patient_id)))))
                        ELSE UPPER(LTRIM(RTRIM(patient_id)))
                     END
        ),
        -- SAE Safety
        sae_safety_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN patient_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(patient_id, 9, LEN(patient_id)))))
                    ELSE UPPER(LTRIM(RTRIM(patient_id)))
                END AS norm_subject_id,
                COUNT(*) AS pending_safety_count
            FROM silver.sae_dashboard_safety
            WHERE UPPER(LTRIM(RTRIM(ISNULL(review_status, '')))) != 'COMPLETED'
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN patient_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(patient_id, 9, LEN(patient_id)))))
                        ELSE UPPER(LTRIM(RTRIM(patient_id)))
                     END
        ),
        -- Non-Conformant Pages
        nc_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                COUNT(DISTINCT folder_name + '|' + page_name) AS non_conformant_page_count
            FROM silver.cpid_edc_non_conformant
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- SDV Verification
        sdv_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                COUNT(*) AS require_verification_count,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(verification_status))) LIKE '%VERIF%' THEN 1 ELSE 0 END) AS verified_count
            FROM silver.cpid_edc_sdv
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- Freeze Status
        frz_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(freeze_status))) = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(freeze_status))) != 'FROZEN' OR freeze_status IS NULL THEN 1 ELSE 0 END) AS not_frozen_count
            FROM silver.cpid_edc_crf_freeze
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- Lock Status
        lck_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(lock_status))) = 'LOCKED' THEN 1 ELSE 0 END) AS locked_count,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(lock_status))) != 'LOCKED' OR lock_status IS NULL THEN 1 ELSE 0 END) AS unlocked_count
            FROM silver.cpid_edc_crf_locked
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- Protocol Deviations
        pd_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(pd_status))) = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_count,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(pd_status))) = 'PROPOSED' THEN 1 ELSE 0 END) AS proposed_count
            FROM silver.cpid_edc_query_protocol_deviation
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- PI Signatures
        sig_calc AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                SUM(CASE WHEN date_last_pi_sign IS NOT NULL THEN 1 ELSE 0 END) AS signed_count,
                SUM(CASE WHEN no_of_days > 0 AND no_of_days <= 45 THEN 1 ELSE 0 END) AS overdue_45_count,
                SUM(CASE WHEN no_of_days > 45 AND no_of_days <= 90 THEN 1 ELSE 0 END) AS overdue_45_90_count,
                SUM(CASE WHEN no_of_days > 90 THEN 1 ELSE 0 END) AS overdue_90_count,
                SUM(CASE WHEN UPPER(LTRIM(RTRIM(audit_action))) LIKE '%BROKEN%' THEN 1 ELSE 0 END) AS broken_signatures_count,
                SUM(CASE WHEN date_last_pi_sign IS NULL AND UPPER(LTRIM(RTRIM(page_require_signature))) LIKE '%YES%' THEN 1 ELSE 0 END) AS never_signed_count
            FROM silver.cpid_edc_pi_signature_report
            GROUP BY UPPER(LTRIM(RTRIM(study_id))),
                     CASE 
                        WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                        ELSE UPPER(LTRIM(RTRIM(subject_id)))
                     END
        ),
        -- CRFs with issues: Pre-aggregate all pages with issues
        pages_with_issues_union AS (
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                folder_name + '|' + form_name AS page_key
            FROM silver.cpid_edc_query_report_cra_action
            UNION
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                folder_name + '|' + form_name AS page_key
            FROM silver.cpid_edc_query_report_site_action
            UNION
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                folder_name + '|' + form_name AS page_key
            FROM silver.cpid_edc_query_report_cumulative
            UNION
            SELECT 
                UPPER(LTRIM(RTRIM(study_id))) AS norm_study_id,
                CASE 
                    WHEN subject_id LIKE 'Subject %' THEN UPPER(LTRIM(RTRIM(SUBSTRING(subject_id, 9, LEN(subject_id)))))
                    ELSE UPPER(LTRIM(RTRIM(subject_id)))
                END AS norm_subject_id,
                folder_name + '|' + page_name AS page_key
            FROM silver.cpid_edc_non_conformant
        ),
        crfs_with_issues_calc AS (
            SELECT 
                norm_study_id,
                norm_subject_id,
                COUNT(DISTINCT page_key) AS crfs_with_issues_count
            FROM pages_with_issues_union
            GROUP BY norm_study_id, norm_subject_id
        )

        -- Final INSERT
        INSERT INTO silver.cpid_edc_subject_metrics  (
            study_id, region, country, site_id, subject_id, latest_visit, subject_status,
            missing_visits, missing_pages, coded_terms, uncoded_terms, 
            open_issues_lnr, open_issues_edrr, inactivated_forms, 
            esae_review_dm, esae_review_safety,
            expected_visits, pages_entered, pages_non_conformant,
            crfs_with_issues, crfs_clean, percent_clean_crf,
            dm_queries, clinical_queries, medical_queries, site_queries,
            field_monitor_queries, coding_queries, safety_queries, total_queries,
            crfs_require_verification, forms_verified,
            crfs_frozen, crfs_not_frozen, crfs_locked, crfs_unlocked,
            pds_confirmed, pds_proposed,
            crfs_signed, crfs_overdue_45, crfs_overdue_45_90, crfs_overdue_90,
            broken_signatures, crfs_never_signed,
            dwh_create_date
        )
        SELECT
            base.norm_study_id AS study_id,
            UPPER(LTRIM(RTRIM(ISNULL(base.region, 'NA')))) AS region,
            LTRIM(RTRIM(ISNULL(base.country, 'NA'))) AS country,
            CASE 
                WHEN base.site_id IS NULL OR LTRIM(RTRIM(base.site_id)) = '' THEN 'Site NA'
                WHEN UPPER(LTRIM(base.site_id)) LIKE 'SITE%' THEN LTRIM(RTRIM(base.site_id))
                ELSE 'Site ' + LTRIM(RTRIM(base.site_id))
            END AS site_id,
            'Subject ' + base.norm_subject_id AS subject_id,
            LTRIM(RTRIM(ISNULL(base.latest_visit, 'NA'))) AS latest_visit,
            LTRIM(RTRIM(ISNULL(base.subject_status, 'Unknown'))) AS subject_status,
            
            -- Calculated fields
            CAST(ISNULL(mv.missing_visit_count, 0) AS int) AS missing_visits,
            CAST(ISNULL(mp.missing_page_count, 0) AS int) AS missing_pages,
            CAST(ISNULL(coded.coded_count, 0) AS int) AS coded_terms,
            CAST(ISNULL(uncoded.uncoded_count, 0) as int) AS uncoded_terms,
            CAST(ISNULL(lnr.open_lnr_count, 0) AS int) AS open_issues_lnr,  -- FIXED!
            CAST(ISNULL(edrr.total_open_issue_count_per_subject, 0) AS int) AS open_issues_edrr,
            CAST(ISNULL(inact.inactivated_count, 0) AS int) AS inactivated_forms,
            CAST(ISNULL(sae_dm.pending_dm_count, 0) AS int) AS esae_review_dm,
            CAST(ISNULL(sae_safety.pending_safety_count, 0) AS int) AS esae_review_safety,
            
            -- Numeric fields from bronze
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.expected_visits)), '') AS INT), 0) AS expected_visits,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.pages_entered)), '') AS INT), 0) AS pages_entered,
            ISNULL(nc.non_conformant_page_count, 0) AS pages_non_conformant,
            
            -- *** CALCULATED FIELDS (NOT IN CSV) ***
            ISNULL(cwi.crfs_with_issues_count, 0) AS crfs_with_issues,
            
            -- crfs_clean = pages_entered - crfs_with_issues
            CASE 
                WHEN ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.pages_entered)), '') AS INT), 0) > 0
                THEN ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.pages_entered)), '') AS INT), 0) - ISNULL(cwi.crfs_with_issues_count, 0)
                ELSE 0
            END AS crfs_clean,
            
            -- percent_clean_crf = (crfs_clean / pages_entered) * 100
            CASE 
                WHEN ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.pages_entered)), '') AS INT), 0) > 0
                THEN CAST((
                    CAST((ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.pages_entered)), '') AS INT), 0) - ISNULL(cwi.crfs_with_issues_count, 0)) AS FLOAT) 
                    / CAST(ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.pages_entered)), '') AS INT), 0) AS FLOAT)
                ) * 100 AS decimal(18,2))
                ELSE 0.0
            END AS percent_clean_crf,
            
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.dm_queries)), '') AS INT), 0) AS dm_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.clinical_queries)), '') AS INT), 0) AS clinical_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.medical_queries)), '') AS INT), 0) AS medical_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.site_queries)), '') AS INT), 0) AS site_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.field_monitor_queries)), '') AS INT), 0) AS field_monitor_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.coding_queries)), '') AS INT), 0) AS coding_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.safety_queries)), '') AS INT), 0) AS safety_queries,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(base.total_queries)), '') AS INT), 0) AS total_queries,
            
            -- More calculated fields
            ISNULL(sdv.require_verification_count, 0) AS crfs_require_verification,
            ISNULL(sdv.verified_count, 0) AS forms_verified,
            ISNULL(frz.frozen_count, 0) AS crfs_frozen,
            ISNULL(frz.not_frozen_count, 0) AS crfs_not_frozen,
            ISNULL(lck.locked_count, 0) AS crfs_locked,
            ISNULL(lck.unlocked_count, 0) AS crfs_unlocked,
            ISNULL(pd.confirmed_count, 0) AS pds_confirmed,
            ISNULL(pd.proposed_count, 0) AS pds_proposed,
            ISNULL(sig.signed_count, 0) AS crfs_signed,
            ISNULL(sig.overdue_45_count, 0) AS crfs_overdue_45,
            ISNULL(sig.overdue_45_90_count, 0) AS crfs_overdue_45_90,
            ISNULL(sig.overdue_90_count, 0) AS crfs_overdue_90,
            ISNULL(sig.broken_signatures_count, 0) AS broken_signatures,
            ISNULL(sig.never_signed_count, 0) AS crfs_never_signed,
            
            GETDATE() AS dwh_create_date

        FROM normalized_base base
        LEFT JOIN mv_calc mv ON base.norm_study_id = mv.norm_study_id AND base.norm_subject_id = mv.norm_subject_id
        LEFT JOIN mp_calc mp ON base.norm_study_id = mp.norm_study_id AND base.norm_subject_id = mp.norm_subject_id
        LEFT JOIN coded_calc coded ON base.norm_study_id = coded.norm_study_id AND base.norm_subject_id = coded.norm_subject_id
        LEFT JOIN uncoded_calc uncoded ON base.norm_study_id = uncoded.norm_study_id AND base.norm_subject_id = uncoded.norm_subject_id
        LEFT JOIN lnr_calc lnr ON base.norm_study_id = lnr.norm_study_id AND base.norm_subject_id = lnr.norm_subject_id
        LEFT JOIN edrr_calc edrr ON base.norm_study_id = edrr.norm_study_id AND base.norm_subject_id = edrr.norm_subject_id
        LEFT JOIN inact_calc inact ON base.norm_study_id = inact.norm_study_id AND base.norm_subject_id = inact.norm_subject_id
        LEFT JOIN sae_dm_calc sae_dm ON base.norm_study_id = sae_dm.norm_study_id AND base.norm_subject_id = sae_dm.norm_subject_id
        LEFT JOIN sae_safety_calc sae_safety ON base.norm_study_id = sae_safety.norm_study_id AND base.norm_subject_id = sae_safety.norm_subject_id
        LEFT JOIN nc_calc nc ON base.norm_study_id = nc.norm_study_id AND base.norm_subject_id = nc.norm_subject_id
        LEFT JOIN sdv_calc sdv ON base.norm_study_id = sdv.norm_study_id AND base.norm_subject_id = sdv.norm_subject_id
        LEFT JOIN frz_calc frz ON base.norm_study_id = frz.norm_study_id AND base.norm_subject_id = frz.norm_subject_id
        LEFT JOIN lck_calc lck ON base.norm_study_id = lck.norm_study_id AND base.norm_subject_id = lck.norm_subject_id
        LEFT JOIN pd_calc pd ON base.norm_study_id = pd.norm_study_id AND base.norm_subject_id = pd.norm_subject_id
        LEFT JOIN sig_calc sig ON base.norm_study_id = sig.norm_study_id AND base.norm_subject_id = sig.norm_subject_id
        LEFT JOIN crfs_with_issues_calc cwi ON base.norm_study_id = cwi.norm_study_id AND base.norm_subject_id = cwi.norm_subject_id;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@rows_affected AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'CPID EDC Subject Metrics Load Completed Successfully';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING CPID EDC SUBJECT METRICS LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Line Number   : ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '================================================';

        THROW;
    END CATCH
END;
GO
