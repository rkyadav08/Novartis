/*
===============================================================================
Stored Procedure: sp_get_priority_actions
===============================================================================
Purpose: Generate prioritized action list for DQT/CRA daily tasks
         Returns actionable items sorted by priority and due date

Priority Levels:
- P1 (CRITICAL): Safety issues, signatures >90 days overdue
- P2 (HIGH): Open queries >30 days, missing visits >14 days
- P3 (MEDIUM): SDV pending, coding required, queries 7-30 days
- P4 (LOW): Routine follow-ups
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.sp_get_priority_actions
    @study_id NVARCHAR(50) = NULL,
    @site_id NVARCHAR(50) = NULL,
    @responsible_party NVARCHAR(50) = NULL,  -- 'CRA', 'DM', 'Safety', 'Coder', 'Investigator'
    @priority NVARCHAR(10) = NULL,  -- 'P1', 'P2', 'P3', 'P4'
    @top_n INT = 100
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH AllActions AS (
        -- P1: Safety Queries (CRITICAL)
        SELECT 
            'P1' AS priority,
            1 AS sort_order,
            'Safety Query - Immediate Resolution Required' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(safety_queries AS VARCHAR), ' open safety queries require immediate attention') AS action_description,
            'Safety Team' AS responsible_party,
            'Within 24 hours' AS due_date,
            safety_queries AS item_count,
            'SAFETY_QUERY' AS action_category
        FROM gold.fact_subject_metrics
        WHERE safety_queries > 0

        UNION ALL

        -- P1: Signatures Overdue >90 days (CRITICAL)
        SELECT 
            'P1' AS priority,
            2 AS sort_order,
            'PI Signature Critically Overdue (>90 days)' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(crfs_overdue_90 AS VARCHAR), ' CRFs need PI signature - regulatory compliance risk') AS action_description,
            'Investigator' AS responsible_party,
            'Within 48 hours' AS due_date,
            crfs_overdue_90 AS item_count,
            'SIGNATURE_OVERDUE' AS action_category
        FROM gold.fact_subject_metrics
        WHERE crfs_overdue_90 > 0

        UNION ALL

        -- P2: Open Queries >30 days (HIGH)
        SELECT 
            'P2' AS priority,
            3 AS sort_order,
            'Query Aging >30 Days' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(total_queries AS VARCHAR), ' queries open - follow up with site') AS action_description,
            'CRA' AS responsible_party,
            'Within 1 week' AS due_date,
            total_queries AS item_count,
            'QUERY_AGING' AS action_category
        FROM gold.fact_subject_metrics
        WHERE total_queries > 5  -- Sites with significant query backlog

        UNION ALL

        -- P2: Missing Visits >14 days (HIGH)
        SELECT 
            'P2' AS priority,
            4 AS sort_order,
            'Missing Visit Follow-up Required' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(missing_visits AS VARCHAR), ' scheduled visits missing - contact site') AS action_description,
            'CRA' AS responsible_party,
            'Within 1 week' AS due_date,
            missing_visits AS item_count,
            'MISSING_VISIT' AS action_category
        FROM gold.fact_subject_metrics
        WHERE missing_visits > 2

        UNION ALL

        -- P2: Signatures Overdue 45-90 days (HIGH)
        SELECT 
            'P2' AS priority,
            5 AS sort_order,
            'PI Signature Overdue (45-90 days)' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(crfs_overdue_45_90 AS VARCHAR), ' CRFs approaching critical signature deadline') AS action_description,
            'Investigator' AS responsible_party,
            'Within 1 week' AS due_date,
            crfs_overdue_45_90 AS item_count,
            'SIGNATURE_WARNING' AS action_category
        FROM gold.fact_subject_metrics
        WHERE crfs_overdue_45_90 > 0

        UNION ALL

        -- P3: SDV Pending (MEDIUM)
        SELECT 
            'P3' AS priority,
            6 AS sort_order,
            'Source Data Verification Pending' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(crfs_require_sdv - forms_verified AS VARCHAR), ' CRFs require SDV') AS action_description,
            'CRA' AS responsible_party,
            'Next monitoring visit' AS due_date,
            crfs_require_sdv - forms_verified AS item_count,
            'SDV_PENDING' AS action_category
        FROM gold.fact_subject_metrics
        WHERE crfs_require_sdv > forms_verified

        UNION ALL

        -- P3: Coding Required (MEDIUM)
        SELECT 
            'P3' AS priority,
            7 AS sort_order,
            'Medical/Drug Coding Required' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(uncoded_terms AS VARCHAR), ' terms require MedDRA/WHODrug coding') AS action_description,
            'Coder' AS responsible_party,
            'Within 2 weeks' AS due_date,
            uncoded_terms AS item_count,
            'CODING_REQUIRED' AS action_category
        FROM gold.fact_subject_metrics
        WHERE uncoded_terms > 0

        UNION ALL

        -- P3: Non-conformant Data (MEDIUM)
        SELECT 
            'P3' AS priority,
            8 AS sort_order,
            'Non-Conformant Data Requires Review' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(pages_non_conformant AS VARCHAR), ' pages with non-conformant data') AS action_description,
            'DM' AS responsible_party,
            'Within 2 weeks' AS due_date,
            pages_non_conformant AS item_count,
            'NON_CONFORMANT' AS action_category
        FROM gold.fact_subject_metrics
        WHERE pages_non_conformant > 0

        UNION ALL

        -- P3: Lab Issues (MEDIUM)
        SELECT 
            'P3' AS priority,
            9 AS sort_order,
            'Lab Data Issues - Missing Ranges/Names' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(open_issues_lnr AS VARCHAR), ' lab issues require resolution') AS action_description,
            'CRA' AS responsible_party,
            'Within 2 weeks' AS due_date,
            open_issues_lnr AS item_count,
            'LAB_ISSUES' AS action_category
        FROM gold.fact_subject_metrics
        WHERE open_issues_lnr > 0

        UNION ALL

        -- P4: Protocol Deviations to Confirm (LOW)
        SELECT 
            'P4' AS priority,
            10 AS sort_order,
            'Protocol Deviation - Pending Confirmation' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(pds_proposed AS VARCHAR), ' protocol deviations pending confirmation') AS action_description,
            'DM' AS responsible_party,
            'Within 30 days' AS due_date,
            pds_proposed AS item_count,
            'PD_PENDING' AS action_category
        FROM gold.fact_subject_metrics
        WHERE pds_proposed > 0

        UNION ALL

        -- P4: EDRR Issues (LOW)
        SELECT 
            'P4' AS priority,
            11 AS sort_order,
            'Third Party Data Reconciliation' AS action_type,
            study_id,
            site_id,
            subject_id,
            region,
            country,
            CONCAT(CAST(open_issues_edrr AS VARCHAR), ' EDRR issues pending reconciliation') AS action_description,
            'DM' AS responsible_party,
            'Within 30 days' AS due_date,
            open_issues_edrr AS item_count,
            'EDRR_ISSUES' AS action_category
        FROM gold.fact_subject_metrics
        WHERE open_issues_edrr > 0
    )
    SELECT
        priority,
        action_type,
        study_id,
        site_id,
        subject_id,
        region,
        country,
        action_description,
        responsible_party,
        due_date,
        item_count,
        action_category,
        -- Priority color for UI
        CASE priority
            WHEN 'P1' THEN '#DC2626'  -- Red
            WHEN 'P2' THEN '#F97316'  -- Orange
            WHEN 'P3' THEN '#EAB308'  -- Yellow
            WHEN 'P4' THEN '#22C55E'  -- Green
        END AS priority_color
    FROM AllActions
    WHERE (@study_id IS NULL OR study_id = @study_id)
      AND (@site_id IS NULL OR site_id = @site_id)
      AND (@responsible_party IS NULL OR responsible_party LIKE '%' + @responsible_party + '%')
      AND (@priority IS NULL OR priority = @priority)
      AND item_count > 0
    ORDER BY sort_order, item_count DESC;
END;
GO

/*
-- Usage Examples:
EXEC gold.sp_get_priority_actions;  -- All actions
EXEC gold.sp_get_priority_actions @priority = 'P1';  -- Critical only
EXEC gold.sp_get_priority_actions @responsible_party = 'CRA';  -- CRA tasks
EXEC gold.sp_get_priority_actions @site_id = 'SITE 888', @top_n = 20;  -- Specific site
*/