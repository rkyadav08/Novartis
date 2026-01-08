/*
===============================================================================
Stored Procedure: sp_get_dashboard_data
===============================================================================
Purpose: Single API endpoint for dashboard to fetch all required data
         Returns multiple result sets for different dashboard components

Result Sets:
1. Study Summary (KPI cards)
2. Regional Performance (Map data)
3. Site Performance (Table/Chart)
4. Trend Data (if historical data available)
5. Top Action Items
6. Query Distribution
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.sp_get_dashboard_data
    @study_id NVARCHAR(50) = NULL,
    @region NVARCHAR(50) = NULL,
    @country NVARCHAR(50) = NULL,
    @site_id NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- =====================================================
    -- RESULT SET 1: Study Summary (KPI Cards)
    -- =====================================================
    SELECT 
        COALESCE(@study_id, 'ALL STUDIES') AS study_id,
        COUNT(DISTINCT study_id) AS total_studies,
        COUNT(DISTINCT region) AS total_regions,
        COUNT(DISTINCT country) AS total_countries,
        COUNT(DISTINCT site_id) AS total_sites,
        COUNT(DISTINCT subject_id) AS total_subjects,
        SUM(is_clean_patient) AS clean_subjects,
        CAST(CAST(SUM(is_clean_patient) AS FLOAT) / NULLIF(COUNT(DISTINCT subject_id), 0) * 100 AS DECIMAL(5,2)) AS pct_clean,
        CAST(AVG(data_quality_index) AS DECIMAL(5,2)) AS avg_dqi,
        SUM(total_queries) AS total_open_queries,
        SUM(safety_queries) AS safety_queries,
        SUM(missing_visits) AS total_missing_visits,
        SUM(uncoded_terms) AS total_uncoded_terms,
        CAST(CAST(SUM(forms_verified) AS FLOAT) / NULLIF(SUM(crfs_require_sdv), 0) * 100 AS DECIMAL(5,2)) AS sdv_completion_pct,
        SUM(crfs_overdue_90) AS critical_signatures,
        SUM(pds_confirmed) AS protocol_deviations,
        -- Readiness
        CASE 
            WHEN AVG(data_quality_index) >= 90 AND SUM(safety_queries) = 0 AND SUM(uncoded_terms) = 0 THEN 'Ready'
            WHEN AVG(data_quality_index) >= 75 THEN 'Near Ready'
            ELSE 'Not Ready'
        END AS submission_readiness
    FROM gold.fact_subject_metrics
    WHERE (@study_id IS NULL OR study_id = @study_id)
      AND (@region IS NULL OR region = @region)
      AND (@country IS NULL OR country = @country)
      AND (@site_id IS NULL OR site_id = @site_id);

    -- =====================================================
    -- RESULT SET 2: Regional Performance (For Map)
    -- =====================================================
    SELECT 
        region,
        COUNT(DISTINCT country) AS countries,
        COUNT(DISTINCT site_id) AS sites,
        COUNT(DISTINCT subject_id) AS subjects,
        SUM(is_clean_patient) AS clean_subjects,
        CAST(AVG(data_quality_index) AS DECIMAL(5,2)) AS avg_dqi,
        SUM(total_queries) AS open_queries,
        CASE 
            WHEN AVG(data_quality_index) >= 85 THEN 'good'
            WHEN AVG(data_quality_index) >= 70 THEN 'warning'
            ELSE 'critical'
        END AS status
    FROM gold.fact_subject_metrics
    WHERE (@study_id IS NULL OR study_id = @study_id)
    GROUP BY region
    ORDER BY avg_dqi DESC;

    -- =====================================================
    -- RESULT SET 3: Country Performance
    -- =====================================================
    SELECT 
        region,
        country,
        COUNT(DISTINCT site_id) AS sites,
        COUNT(DISTINCT subject_id) AS subjects,
        SUM(is_clean_patient) AS clean_subjects,
        CAST(CAST(SUM(is_clean_patient) AS FLOAT) / NULLIF(COUNT(DISTINCT subject_id), 0) * 100 AS DECIMAL(5,2)) AS pct_clean,
        CAST(AVG(data_quality_index) AS DECIMAL(5,2)) AS avg_dqi,
        SUM(total_queries) AS open_queries,
        SUM(missing_visits) AS missing_visits
    FROM gold.fact_subject_metrics
    WHERE (@study_id IS NULL OR study_id = @study_id)
      AND (@region IS NULL OR region = @region)
    GROUP BY region, country
    ORDER BY region, avg_dqi DESC;

    -- =====================================================
    -- RESULT SET 4: Site Performance (Top 20 + Filtered)
    -- =====================================================
    SELECT TOP 50
        study_id,
        site_id,
        region,
        country,
        COUNT(DISTINCT subject_id) AS subjects,
        SUM(is_clean_patient) AS clean_subjects,
        CAST(CAST(SUM(is_clean_patient) AS FLOAT) / NULLIF(COUNT(DISTINCT subject_id), 0) * 100 AS DECIMAL(5,2)) AS pct_clean,
        CAST(AVG(data_quality_index) AS DECIMAL(5,2)) AS avg_dqi,
        SUM(total_queries) AS open_queries,
        SUM(missing_visits) AS missing_visits,
        SUM(crfs_overdue_90) AS sig_overdue,
        CAST(CAST(SUM(forms_verified) AS FLOAT) / NULLIF(SUM(crfs_require_sdv), 0) * 100 AS DECIMAL(5,2)) AS sdv_pct,
        -- Risk calculation
        CASE 
            WHEN AVG(data_quality_index) < 50 OR SUM(safety_queries) > 0 THEN 'CRITICAL'
            WHEN AVG(data_quality_index) < 70 OR SUM(crfs_overdue_90) > 0 THEN 'HIGH'
            WHEN AVG(data_quality_index) < 85 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level
    FROM gold.fact_subject_metrics
    WHERE (@study_id IS NULL OR study_id = @study_id)
      AND (@region IS NULL OR region = @region)
      AND (@country IS NULL OR country = @country)
      AND (@site_id IS NULL OR site_id = @site_id)
    GROUP BY study_id, site_id, region, country
    ORDER BY AVG(data_quality_index) ASC;  -- Worst first

    -- =====================================================
    -- RESULT SET 5: Top Action Items (Priority Queue)
    -- =====================================================
    SELECT TOP 20
        priority,
        action_type,
        study_id,
        site_id,
        subject_id,
        region,
        country,
        action_description,
        responsible_party,
        -- Add priority_color dynamically
        CASE priority 
            WHEN 'CRITICAL' THEN '#DC2626'
            WHEN 'HIGH' THEN '#F97316'
            WHEN 'MEDIUM' THEN '#EAB308'
            ELSE '#22C55E'
        END AS priority_color
    FROM gold.vw_action_items
    WHERE (@study_id IS NULL OR study_id = @study_id)
      AND (@region IS NULL OR region = @region)
      AND (@country IS NULL OR country = @country)
      AND (@site_id IS NULL OR site_id = @site_id)
    ORDER BY 
        CASE priority WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END;

    -- =====================================================
    -- RESULT SET 6: Query Distribution by Type
    -- =====================================================
    SELECT 
        'DM' AS query_type, SUM(dm_queries) AS count, '#3B82F6' AS color
    FROM gold.fact_subject_metrics
    WHERE (@study_id IS NULL OR study_id = @study_id)
    UNION ALL
    SELECT 'Clinical', SUM(clinical_queries), '#10B981'
    FROM gold.fact_subject_metrics WHERE (@study_id IS NULL OR study_id = @study_id)
    UNION ALL
    SELECT 'Medical', SUM(medical_queries), '#8B5CF6'
    FROM gold.fact_subject_metrics WHERE (@study_id IS NULL OR study_id = @study_id)
    UNION ALL
    SELECT 'Safety', SUM(safety_queries), '#EF4444'
    FROM gold.fact_subject_metrics WHERE (@study_id IS NULL OR study_id = @study_id)
    UNION ALL
    SELECT 'Coding', SUM(coding_queries), '#F59E0B'
    FROM gold.fact_subject_metrics WHERE (@study_id IS NULL OR study_id = @study_id)
    UNION ALL
    SELECT 'Site', SUM(site_queries), '#6366F1'
    FROM gold.fact_subject_metrics WHERE (@study_id IS NULL OR study_id = @study_id)
    UNION ALL
    SELECT 'Field Monitor', SUM(field_monitor_queries), '#EC4899'
    FROM gold.fact_subject_metrics WHERE (@study_id IS NULL OR study_id = @study_id);

    -- =====================================================
    -- RESULT SET 7: DQI Distribution (for histogram)
    -- =====================================================
    SELECT 
        CASE 
            WHEN data_quality_index >= 90 THEN '90-100 (Excellent)'
            WHEN data_quality_index >= 75 THEN '75-89 (Good)'
            WHEN data_quality_index >= 50 THEN '50-74 (Fair)'
            WHEN data_quality_index >= 25 THEN '25-49 (Poor)'
            ELSE '0-24 (Critical)'
        END AS dqi_range,
        COUNT(*) AS subject_count,
        CASE 
            WHEN data_quality_index >= 90 THEN '#22C55E'
            WHEN data_quality_index >= 75 THEN '#84CC16'
            WHEN data_quality_index >= 50 THEN '#EAB308'
            WHEN data_quality_index >= 25 THEN '#F97316'
            ELSE '#EF4444'
        END AS color
    FROM gold.fact_subject_metrics
    WHERE (@study_id IS NULL OR study_id = @study_id)
    GROUP BY 
        CASE 
            WHEN data_quality_index >= 90 THEN '90-100 (Excellent)'
            WHEN data_quality_index >= 75 THEN '75-89 (Good)'
            WHEN data_quality_index >= 50 THEN '50-74 (Fair)'
            WHEN data_quality_index >= 25 THEN '25-49 (Poor)'
            ELSE '0-24 (Critical)'
        END,
        CASE 
            WHEN data_quality_index >= 90 THEN '#22C55E'
            WHEN data_quality_index >= 75 THEN '#84CC16'
            WHEN data_quality_index >= 50 THEN '#EAB308'
            WHEN data_quality_index >= 25 THEN '#F97316'
            ELSE '#EF4444'
        END
    ORDER BY MIN(data_quality_index) DESC;

END;
GO

/*
-- Usage Examples:
EXEC gold.sp_get_dashboard_data;  -- All data
EXEC gold.sp_get_dashboard_data @study_id = 'STUDY001';
EXEC gold.sp_get_dashboard_data @region = 'EU';
EXEC gold.sp_get_dashboard_data  @site_id = 'Site 888';
*/