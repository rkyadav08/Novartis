/*
===============================================================================
Stored Procedure: sp_get_site_risk_score
===============================================================================
Purpose: Calculate risk scores for each site based on multiple factors
         to identify underperforming sites requiring immediate attention

Risk Factors (Weighted):
- Open Queries (20%)
- Missing Visits (15%)
- Missing Pages (15%)
- Non-conformant Data (15%)
- SDV Incomplete (10%)
- Signature Overdue (10%)
- Protocol Deviations (10%)
- Safety Issues (5%)
use datawarehousenovartis
Risk Levels:
- CRITICAL: Score >= 75
- HIGH: Score >= 50
- MEDIUM: Score >= 25
- LOW: Score < 25
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.sp_get_site_risk_score
    @study_id NVARCHAR(50) = NULL,
    @region NVARCHAR(50) = NULL,
    @country NVARCHAR(50) = NULL,
    @risk_level NVARCHAR(20) = NULL  -- Filter: 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
AS
BEGIN
    SET NOCOUNT ON;

    WITH SiteMetrics AS (
        SELECT 
            study_id,
            site_id,
            region,
            country,
            COUNT(DISTINCT subject_id) AS total_subjects,
            SUM(is_clean_patient) AS clean_subjects,
            
            -- Raw metrics
            SUM(total_queries) AS total_queries,
            SUM(missing_visits) AS total_missing_visits,
            SUM(missing_pages) AS total_missing_pages,
            SUM(pages_non_conformant) AS total_non_conformant,
            SUM(crfs_require_sdv - forms_verified) AS total_sdv_pending,
            SUM(crfs_overdue_90) AS total_sig_overdue,
            SUM(pds_confirmed) AS total_pds,
            SUM(safety_queries) AS total_safety_issues,
            SUM(expected_visits) AS total_expected_visits,
            SUM(crfs_require_sdv) AS total_crfs_sdv,
            
            -- Averages
            AVG(data_quality_index) AS avg_dqi
            
        FROM  gold.fact_subject_metrics
        WHERE (@study_id IS NULL OR study_id = @study_id)
          AND (@region IS NULL OR region = @region)
          AND (@country IS NULL OR country = @country)
        GROUP BY study_id, site_id, region, country
    ),
    RiskCalculation AS (
        SELECT 
            study_id,
            site_id,
            region,
            country,
            total_subjects,
            clean_subjects,
            avg_dqi,
            
            -- Individual risk components (0-100 scale)
            CASE WHEN total_subjects > 0 
                THEN LEAST(100, (CAST(total_queries AS FLOAT) / total_subjects) * 10)
                ELSE 0 
            END AS query_risk,
            
            CASE WHEN total_expected_visits > 0 
                THEN LEAST(100, (CAST(total_missing_visits AS FLOAT) / total_expected_visits) * 100)
                ELSE 0 
            END AS visit_risk,
            
            CASE WHEN total_subjects > 0 
                THEN LEAST(100, (CAST(total_missing_pages AS FLOAT) / total_subjects) * 5)
                ELSE 0 
            END AS page_risk,
            
            CASE WHEN total_subjects > 0 
                THEN LEAST(100, (CAST(total_non_conformant AS FLOAT) / total_subjects) * 10)
                ELSE 0 
            END AS conformant_risk,
            
            CASE WHEN total_crfs_sdv > 0 
                THEN LEAST(100, (CAST(total_sdv_pending AS FLOAT) / total_crfs_sdv) * 100)
                ELSE 0 
            END AS sdv_risk,
            
            CASE WHEN total_subjects > 0 
                THEN LEAST(100, (CAST(total_sig_overdue AS FLOAT) / total_subjects) * 20)
                ELSE 0 
            END AS signature_risk,
            
            CASE WHEN total_subjects > 0 
                THEN LEAST(100, (CAST(total_pds AS FLOAT) / total_subjects) * 15)
                ELSE 0 
            END AS pd_risk,
            
            CASE WHEN total_safety_issues > 0 THEN 100 ELSE 0 END AS safety_risk,
            
            -- Raw counts for display
            total_queries,
            total_missing_visits,
            total_missing_pages,
            total_non_conformant,
            total_sdv_pending,
            total_sig_overdue,
            total_pds,
            total_safety_issues
            
        FROM SiteMetrics
    ),
    FinalScore AS (
        SELECT 
            *,
            -- Weighted risk score
            CAST(
                (query_risk * 0.30) +
                (visit_risk * 0.10) +
                (page_risk * 0.10) +
                (conformant_risk * 0.05) +
                (sdv_risk * 0.30) +
                (signature_risk * 0.05) +
                (pd_risk * 0.05) +
                (safety_risk * 0.05)
            AS DECIMAL(5,2)) AS risk_score,
            
            -- Risk level
            CASE 
                WHEN (query_risk * 0.30) +
                (visit_risk * 0.10) +
                (page_risk * 0.10) +
                (conformant_risk * 0.05) +
                (sdv_risk * 0.30) +
                (signature_risk * 0.05) +
                (pd_risk * 0.05) +
                (safety_risk * 0.05) >= 75 THEN 'CRITICAL'
                WHEN (query_risk * 0.30) +
                (visit_risk * 0.10) +
                (page_risk * 0.10) +
                (conformant_risk * 0.05) +
                (sdv_risk * 0.30) +
                (signature_risk * 0.05) +
                (pd_risk * 0.05) +
                (safety_risk * 0.05) >= 50 THEN 'HIGH'
                WHEN (query_risk * 0.30) +
                (visit_risk * 0.10) +
                (page_risk * 0.10) +
                (conformant_risk * 0.05) +
                (sdv_risk * 0.30) +
                (signature_risk * 0.05) +
                (pd_risk * 0.05) +
                (safety_risk * 0.05) >= 25 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risk_level
        FROM RiskCalculation
    )
    SELECT 
        study_id,
        site_id,
        region,
        country,
        total_subjects,
        clean_subjects,
        CAST(avg_dqi AS DECIMAL(5,2)) AS avg_data_quality_index,
        risk_score,
        risk_level,
        
        -- Risk breakdown
        CAST(query_risk AS DECIMAL(5,2)) AS query_risk_pct,
        CAST(visit_risk AS DECIMAL(5,2)) AS visit_risk_pct,
        CAST(sdv_risk AS DECIMAL(5,2)) AS sdv_risk_pct,
        CAST(signature_risk AS DECIMAL(5,2)) AS signature_risk_pct,
        
        -- Action counts
        total_queries AS open_queries,
        total_missing_visits AS missing_visits,
        total_sdv_pending AS sdv_pending,
        total_sig_overdue AS signatures_overdue,
        total_pds AS protocol_deviations,
        total_safety_issues AS safety_issues,
        
        -- Recommended actions
        CASE 
            WHEN risk_level = 'CRITICAL' THEN 'IMMEDIATE INTERVENTION REQUIRED - Schedule urgent site visit'
            WHEN risk_level = 'HIGH' THEN 'Schedule site review within 1 week'
            WHEN risk_level = 'MEDIUM' THEN 'Monitor closely - Review in next scheduled visit'
            ELSE 'Standard monitoring'
        END AS recommended_action
        
    FROM FinalScore
    WHERE (@risk_level IS NULL OR risk_level = @risk_level)
    ORDER BY risk_score DESC, total_subjects DESC;
END;
GO

/*
-- Usage Examples:
EXEC gold.sp_get_site_risk_score;  -- All sites
EXEC gold.sp_get_site_risk_score @study_id = 'STUDY001';  -- Specific study
EXEC gold.sp_get_site_risk_score @risk_level = 'CRITICAL';  -- Only critical sites
EXEC gold.sp_get_site_risk_score @region = 'EU', @risk_level = 'HIGH';  -- EU high-risk sites
*/