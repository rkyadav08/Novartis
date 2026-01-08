/*
===============================================================================
Stored Procedure: sp_check_submission_readiness
===============================================================================
Purpose: Automated readiness check for interim analysis or regulatory submission
         Evaluates multiple criteria and provides pass/fail status with details

Readiness Criteria:
- Data Quality Index >= 90% (configurable)
- Zero open safety queries
- Zero uncoded terms
- SDV completion >= 95%
- Zero signatures overdue >90 days
- Protocol deviations documented
- Clean patient rate >= 80%
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.sp_check_submission_readiness
    @study_id NVARCHAR(50),
    @min_dqi DECIMAL(5,2) = 90.0,
    @min_sdv_pct DECIMAL(5,2) = 95.0,
    @min_clean_pct DECIMAL(5,2) = 80.0
AS
BEGIN
    SET NOCOUNT ON;

    -- Study Summary
    DECLARE @total_subjects INT,
            @clean_subjects INT,
            @pct_clean DECIMAL(5,2),
            @avg_dqi DECIMAL(5,2),
            @total_queries INT,
            @safety_queries INT,
            @uncoded_terms INT,
            @sdv_required INT,
            @sdv_complete INT,
            @pct_sdv DECIMAL(5,2),
            @sig_overdue_90 INT,
            @total_pds INT,
            @missing_visits INT,
            @missing_pages INT;

    SELECT 
        @total_subjects = COUNT(DISTINCT subject_id),
        @clean_subjects = SUM(is_clean_patient),
        @avg_dqi = AVG(data_quality_index),
        @total_queries = SUM(total_queries),
        @safety_queries = SUM(safety_queries),
        @uncoded_terms = SUM(uncoded_terms),
        @sdv_required = SUM(crfs_require_sdv),
        @sdv_complete = SUM(forms_verified),
        @sig_overdue_90 = SUM(crfs_overdue_90),
        @total_pds = SUM(pds_confirmed),
        @missing_visits = SUM(missing_visits),
        @missing_pages = SUM(missing_pages)
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id;

    SET @pct_clean = CASE WHEN @total_subjects > 0 
        THEN CAST(@clean_subjects AS FLOAT) / @total_subjects * 100 
        ELSE 0 END;
    
    SET @pct_sdv = CASE WHEN @sdv_required > 0 
        THEN CAST(@sdv_complete AS FLOAT) / @sdv_required * 100 
        ELSE 100 END;

    -- Readiness Check Results
    SELECT 
        @study_id AS study_id,
        @total_subjects AS total_subjects,
        
        -- Overall Readiness
        CASE 
            WHEN @avg_dqi >= @min_dqi 
                AND @safety_queries = 0 
                AND @uncoded_terms = 0 
                AND @pct_sdv >= @min_sdv_pct
                AND @sig_overdue_90 = 0
                AND @pct_clean >= @min_clean_pct
            THEN 'READY FOR SUBMISSION'
            WHEN @avg_dqi >= (@min_dqi - 10) 
                AND @safety_queries = 0 
            THEN 'NEAR READY - Minor Issues'
            ELSE 'NOT READY - Action Required'
        END AS readiness_status,

        -- Detailed Checks
        @avg_dqi AS avg_data_quality_index,
        CASE WHEN @avg_dqi >= @min_dqi THEN 'PASS' ELSE 'FAIL' END AS dqi_check,
        CONCAT('Target: ', @min_dqi, '% | Actual: ', CAST(@avg_dqi AS VARCHAR)) AS dqi_detail,

        @pct_clean AS pct_clean_patients,
        CASE WHEN @pct_clean >= @min_clean_pct THEN 'PASS' ELSE 'FAIL' END AS clean_patient_check,
        CONCAT(@clean_subjects, ' of ', @total_subjects, ' subjects clean') AS clean_detail,

        @safety_queries AS open_safety_queries,
        CASE WHEN @safety_queries = 0 THEN 'PASS' ELSE 'FAIL' END AS safety_query_check,

        @uncoded_terms AS uncoded_terms,
        CASE WHEN @uncoded_terms = 0 THEN 'PASS' ELSE 'FAIL' END AS coding_check,

        @pct_sdv AS sdv_completion_pct,
        CASE WHEN @pct_sdv >= @min_sdv_pct THEN 'PASS' ELSE 'FAIL' END AS sdv_check,
        CONCAT(@sdv_complete, ' of ', @sdv_required, ' CRFs verified') AS sdv_detail,

        @sig_overdue_90 AS signatures_overdue_90days,
        CASE WHEN @sig_overdue_90 = 0 THEN 'PASS' ELSE 'FAIL' END AS signature_check,

        @total_queries AS total_open_queries,
        CASE WHEN @total_queries = 0 THEN 'PASS' ELSE 'WARNING' END AS query_check,

        @missing_visits AS missing_visits,
        @missing_pages AS missing_pages,
        @total_pds AS protocol_deviations_documented,

        -- Action Items Summary
        CASE 
            WHEN @safety_queries > 0 THEN CONCAT('CRITICAL: Resolve ', @safety_queries, ' safety queries immediately')
            WHEN @sig_overdue_90 > 0 THEN CONCAT('CRITICAL: ', @sig_overdue_90, ' signatures >90 days overdue')
            WHEN @uncoded_terms > 0 THEN CONCAT('HIGH: Code ', @uncoded_terms, ' pending terms')
            WHEN @pct_sdv < @min_sdv_pct THEN CONCAT('HIGH: Complete SDV - currently at ', CAST(@pct_sdv AS VARCHAR), '%')
            WHEN @total_queries > 0 THEN CONCAT('MEDIUM: Close ', @total_queries, ' open queries')
            ELSE 'No blocking issues'
        END AS primary_blocker,

        GETDATE() AS assessment_date;

    -- Detailed Site-Level Readiness
    PRINT '';
    PRINT '=== Site-Level Readiness Summary ===';
    
    SELECT 
        site_id,
        country,
        COUNT(DISTINCT subject_id) AS subjects,
        SUM(is_clean_patient) AS clean_subjects,
        AVG(data_quality_index) AS avg_dqi,
        SUM(safety_queries) AS safety_queries,
        SUM(uncoded_terms) AS uncoded_terms,
        CASE 
            WHEN AVG(data_quality_index) >= @min_dqi AND SUM(safety_queries) = 0 AND SUM(uncoded_terms) = 0
            THEN 'READY'
            ELSE 'NOT READY'
        END AS site_status
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id
    GROUP BY site_id, country
    ORDER BY AVG(data_quality_index) DESC;

END;
GO

/*
-- Usage Examples:
EXEC gold.sp_check_submission_readiness @study_id = 'STUDY 2';
EXEC gold.sp_check_submission_readiness @study_id = 'STUDY001', @min_dqi = 85.0, @min_sdv_pct = 90.0;
*/