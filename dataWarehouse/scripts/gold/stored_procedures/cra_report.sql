/*
===============================================================================
Stored Procedure: sp_generate_cra_report
===============================================================================
Purpose: Generate comprehensive CRA (Clinical Research Associate) report
         for site monitoring visits

Output Sections:
1. Site Overview & Performance Summary
2. Subject-Level Status
3. Query Metrics & Aging
4. SDV Status
5. Signature Status
6. Action Items for This Visit
7. Comparison with Previous Period (if available)
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.sp_generate_cra_report
    @study_id NVARCHAR(50),
    @site_id NVARCHAR(50),
    @include_subject_detail BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '================================================================';
    PRINT '  CRA MONITORING REPORT';
    PRINT '  Generated: ' + CONVERT(VARCHAR, GETDATE(), 120);
    PRINT '================================================================';
    PRINT '';

    -- =====================================================
    -- SECTION 1: Site Overview
    -- =====================================================
    PRINT '--- SECTION 1: SITE OVERVIEW ---';
    
    SELECT 
        study_id AS [Study],
        site_id AS [Site ID],
        region AS [Region],
        country AS [Country],
        COUNT(DISTINCT subject_id) AS [Total Subjects],
        SUM(CASE WHEN subject_status = 'Ongoing' THEN 1 ELSE 0 END) AS [Ongoing],
        SUM(CASE WHEN subject_status = 'Completed' THEN 1 ELSE 0 END) AS [Completed],
        SUM(CASE WHEN subject_status = 'Discontinued' THEN 1 ELSE 0 END) AS [Discontinued],
        SUM(is_clean_patient) AS [Clean Subjects],
        CAST(AVG(data_quality_index) AS DECIMAL(5,2)) AS [Avg DQI],
        CASE 
            WHEN AVG(data_quality_index) >= 90 THEN 'Excellent'
            WHEN AVG(data_quality_index) >= 75 THEN 'Good'
            WHEN AVG(data_quality_index) >= 50 THEN 'Needs Improvement'
            ELSE 'Critical'
        END AS [Site Rating]
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id
    GROUP BY study_id, site_id, region, country;

    -- =====================================================
    -- SECTION 2: Key Metrics Summary
    -- =====================================================
    PRINT '';
    PRINT '--- SECTION 2: KEY METRICS SUMMARY ---';

    SELECT 
        'Visits' AS [Category],
        SUM(expected_visits) AS [Expected],
        SUM(expected_visits) - SUM(missing_visits) AS [Completed],
        SUM(missing_visits) AS [Missing],
        CAST(CASE WHEN SUM(expected_visits) > 0 
            THEN (1 - CAST(SUM(missing_visits) AS FLOAT)/SUM(expected_visits)) * 100 
            ELSE 100 END AS DECIMAL(5,2)) AS [Completion %]
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id

    UNION ALL

    SELECT 
        'CRF Pages',
        SUM(pages_entered) + SUM(missing_pages),
        SUM(pages_entered),
        SUM(missing_pages),
        CAST(CASE WHEN (SUM(pages_entered) + SUM(missing_pages)) > 0 
            THEN CAST(SUM(pages_entered) AS FLOAT)/(SUM(pages_entered) + SUM(missing_pages)) * 100 
            ELSE 100 END AS DECIMAL(5,2))
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id

    UNION ALL
	
    SELECT 
        'SDV',
        SUM(crfs_require_sdv),
        SUM(forms_verified),
        SUM(crfs_require_sdv) - SUM(forms_verified),
        CAST(CASE WHEN SUM(crfs_require_sdv) > 0 
            THEN CAST(SUM(forms_verified) AS FLOAT)/SUM(crfs_require_sdv) * 100 
            ELSE 100 END AS DECIMAL(5,2))
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id

    UNION ALL

    SELECT 
        'Clean CRFs',
        SUM(crfs_with_issues) + SUM(crfs_clean),
        SUM(crfs_clean),
        SUM(crfs_with_issues),
        AVG(percent_clean_crf)
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id;

    -- =====================================================
    -- SECTION 3: Query Summary
    -- =====================================================
    PRINT '';
    PRINT '--- SECTION 3: QUERY SUMMARY ---';

    SELECT 
        'DM Queries' AS [Query Type], SUM(dm_queries) AS [Open Count]
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id
    UNION ALL
    SELECT 'Clinical Queries', SUM(clinical_queries)
    FROM gold.fact_subject_metrics WHERE study_id = @study_id AND site_id = @site_id
    UNION ALL
    SELECT 'Medical Queries', SUM(medical_queries)
    FROM gold.fact_subject_metrics WHERE study_id = @study_id AND site_id = @site_id
    UNION ALL
    SELECT 'Safety Queries', SUM(safety_queries)
    FROM gold.fact_subject_metrics WHERE study_id = @study_id AND site_id = @site_id
    UNION ALL
    SELECT 'Coding Queries', SUM(coding_queries)
    FROM gold.fact_subject_metrics WHERE study_id = @study_id AND site_id = @site_id
    UNION ALL
    SELECT 'Site Queries', SUM(site_queries)
    FROM gold.fact_subject_metrics WHERE study_id = @study_id AND site_id = @site_id
    UNION ALL
    SELECT '--- TOTAL ---', SUM(total_queries)
    FROM gold.fact_subject_metrics WHERE study_id = @study_id AND site_id = @site_id;

    -- =====================================================
    -- SECTION 4: Signature Status
    -- =====================================================
    PRINT '';
    PRINT '--- SECTION 4: PI SIGNATURE STATUS ---';

    SELECT 
        SUM(crfs_signed) AS [CRFs Signed],
        SUM(crfs_never_signed) AS [Never Signed],
        SUM(crfs_overdue_45) AS [Overdue <45 Days],
        SUM(crfs_overdue_45_90) AS [Overdue 45-90 Days],
        SUM(crfs_overdue_90) AS [Overdue >90 Days (CRITICAL)],
        SUM(broken_signatures) AS [Broken Signatures]
    FROM gold.fact_subject_metrics
    WHERE study_id = @study_id AND site_id = @site_id;

    -- =====================================================
    -- SECTION 5: Subject Detail (Optional)
    -- =====================================================
    IF @include_subject_detail = 1
    BEGIN
        PRINT '';
        PRINT '--- SECTION 5: SUBJECT-LEVEL DETAIL ---';

        SELECT 
            subject_id AS [Subject],
            subject_status AS [Status],
            latest_visit AS [Latest Visit],
            CAST(data_quality_index AS DECIMAL(5,2)) AS [DQI],
            missing_visits AS [Missing Visits],
            missing_pages AS [Missing Pages],
            total_queries AS [Open Queries],
            uncoded_terms AS [Uncoded Terms],
            CASE WHEN is_clean_patient = 1 THEN 'Yes' ELSE 'No' END AS [Clean],
            CASE 
                WHEN data_quality_index >= 90 AND is_clean_patient = 1 THEN '✓ Good'
                WHEN total_queries > 0 OR missing_visits > 0 THEN '⚠ Action Needed'
                ELSE '○ Monitor'
            END AS [Status Flag]
        FROM gold.fact_subject_metrics
        WHERE study_id = @study_id AND site_id = @site_id
        ORDER BY data_quality_index ASC;  -- Worst first
    END

    -- =====================================================
    -- SECTION 6: Action Items for This Visit
    -- =====================================================
    PRINT '';
    PRINT '--- SECTION 6: ACTION ITEMS FOR THIS VISIT ---';

    SELECT 
        priority,
        action_type,
        subject_id,
        action_description,
        responsible_party,
        due_date
    FROM (
        SELECT 
            'P1' AS priority,
            'Safety Query' AS action_type,
            subject_id,
            CONCAT(safety_queries, ' safety queries') AS action_description,
            'Safety Team' AS responsible_party,
            'Immediate' AS due_date,
            1 AS sort_order
        FROM gold.fact_subject_metrics
        WHERE study_id = @study_id AND site_id = @site_id AND safety_queries > 0

        UNION ALL

        SELECT 'P1', 'Signature Overdue >90d', subject_id,
            CONCAT(crfs_overdue_90, ' CRFs critically overdue'),
            'Investigator', 'This Visit', 2
        FROM gold.fact_subject_metrics
        WHERE study_id = @study_id AND site_id = @site_id AND crfs_overdue_90 > 0

        UNION ALL

        SELECT 'P2', 'Missing Visits', subject_id,
            CONCAT(missing_visits, ' visits to follow up'),
            'CRA/Site', 'This Visit', 3
        FROM gold.fact_subject_metrics
        WHERE study_id = @study_id AND site_id = @site_id AND missing_visits > 2

        UNION ALL

        SELECT 'P2', 'Query Resolution', subject_id,
            CONCAT(total_queries, ' queries to address'),
            'Site', 'This Visit', 4
        FROM gold.fact_subject_metrics
        WHERE study_id = @study_id AND site_id = @site_id AND total_queries > 5

        UNION ALL

        SELECT 'P3', 'SDV Required', subject_id,
            CONCAT(crfs_require_sdv - forms_verified, ' CRFs need SDV'),
            'CRA', 'This Visit', 5
        FROM gold.fact_subject_metrics
        WHERE study_id = @study_id AND site_id = @site_id AND crfs_require_sdv > forms_verified
    ) actions
    ORDER BY sort_order, action_description DESC;

    PRINT '';
    PRINT '================================================================';
    PRINT '  END OF REPORT';
    PRINT '================================================================';

END;
GO

/*
-- Usage Examples:
EXEC gold.sp_generate_cra_report @study_id = 'STUDY001', @site_id = 'SITE001';
EXEC gold.sp_generate_cra_report @study_id = 'STUDY001', @site_id = 'SITE001', @include_subject_detail = 0;
*/