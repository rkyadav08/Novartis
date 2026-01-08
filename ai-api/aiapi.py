

import os
import json
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
import google.generativeai as genai
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ============================================================================
# Configuration
# ============================================================================

# Gemini API Configuration - Set your API key here or via environment variable
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

# Database Configuration
DB_CONFIG = {
    "server": os.getenv("DB_SERVER", "SERVER"),
    "database": os.getenv("DB_NAME", "DATABASE"),
    "username": os.getenv("DB_USER", "USERID"),
    "password": os.getenv("DB_PASSWORD", "PASSWORD"),
}

# Initialize Gemini Model
model = genai.GenerativeModel('gemini-2.5-flash')

# Initialize FastAPI
app = FastAPI(
    title="Clinical Trial AI API",
    description="AI-powered insights for clinical trial data using Gemini",
    version="1.0.0"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Database Connection
# ============================================================================

def get_db_connection():
    """Create database connection"""
    try:
        import pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']}"
        )
        return pyodbc.connect(conn_str)
    except ImportError:
        print("Warning: pyodbc not installed. Using mock data.")
        return None


def execute_query(query: str) -> pd.DataFrame:
    """Execute SQL query and return DataFrame"""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            raise Exception(f"Database error: {str(e)}")
    else:
        # Return mock data for testing without database
        return get_mock_data(query)


def get_mock_data(query: str) -> pd.DataFrame:
    """Return mock data for testing"""
    query_lower = query.lower()
    
    if 'agg_site_performance' in query_lower or 'site' in query_lower:
        return pd.DataFrame({
            'study_id': ['STUDY001'] * 5,
            'site_id': ['SITE-001', 'SITE-002', 'SITE-003', 'SITE-004', 'SITE-005'],
            'region': ['US', 'EU', 'EU', 'ASIA', 'US'],
            'country': ['USA', 'Germany', 'France', 'Japan', 'Canada'],
            'total_subjects': [35, 28, 31, 22, 26],
            'clean_subjects': [28, 12, 18, 14, 22],
            'avg_data_quality_index': [88.5, 45.2, 62.8, 58.4, 82.1],
            'total_open_queries': [3, 34, 18, 19, 8]
        })
    elif 'fact_subject_metrics' in query_lower:
        return pd.DataFrame({
            'study_id': ['STUDY001'] * 10,
            'site_id': ['SITE-001'] * 10,
            'subject_id': [f'SUB-{i:04d}' for i in range(1, 11)],
            'data_quality_index': [92, 88, 75, 68, 45, 82, 91, 55, 78, 85],
            'is_clean_patient': [1, 1, 0, 0, 0, 1, 1, 0, 0, 1],
            'total_queries': [0, 1, 5, 8, 15, 2, 0, 12, 4, 1],
            'missing_visits': [0, 0, 2, 3, 5, 1, 0, 4, 1, 0]
        })
    else:
        return pd.DataFrame({
            'result': ['No data available for this query']
        })


# ============================================================================
# Schema Context for Gemini
# ============================================================================

SCHEMA_CONTEXT = """
You are a clinical trial data analyst with access to a SQL Server data warehouse.
The database has a Gold layer with the following key views:

DIMENSION VIEWS:
- gold.dim_region: region_key, region_code, region_name
- gold.dim_country: country_key, region_code, country_code, country_name
- gold.dim_site: site_key, region_code, country_code, site_id, site_name
- gold.dim_study: study_key, study_id, study_name, study_status
- gold.dim_subject: subject_key, study_id, site_id, subject_id, subject_status

FACT VIEWS:
- gold.fact_subject_metrics: Core subject metrics including:
  * study_id, site_id, subject_id, region, country
  * missing_visits, expected_visits, pct_missing_visits
  * missing_pages, pages_entered, percent_clean_crf
  * coded_terms, uncoded_terms, pct_coded_terms
  * total_queries, dm_queries, clinical_queries, medical_queries, safety_queries
  * crfs_require_sdv, forms_verified, pct_sdv_complete
  * crfs_overdue_45, crfs_overdue_45_90, crfs_overdue_90
  * pds_confirmed, pds_proposed
  * data_quality_index (0-100 score, higher is better)
  * is_clean_patient (1=clean, 0=not clean)

- gold.fact_query_metrics: Query tracking with days_since_open, query_status, query_age_bucket
- gold.fact_sdv_status: SDV verification tracking
- gold.fact_missing_visits: Overdue visit tracking with days_outstanding
- gold.fact_sae_dashboard: Safety event tracking

AGGREGATE VIEWS:
- gold.agg_site_performance: Site-level KPIs (avg_data_quality_index, total_open_queries, pct_clean_subjects)
- gold.agg_country_performance: Country-level KPIs
- gold.agg_study_summary: Study executive summary with submission_readiness
- gold.vw_action_items: Prioritized action list (priority, action_type, responsible_party)

Key metrics to know:
- Data Quality Index (DQI): 0-100, higher is better (>=90 excellent, >=75 good, >=50 fair, <50 poor)
- Clean Patient: No missing visits, no queries, all verified and signed
- SDV: Source Data Verification
- CRF: Case Report Form
- PI: Principal Investigator
- PD: Protocol Deviation
"""


# ============================================================================
# Pydantic Models
# ============================================================================

class NLQueryRequest(BaseModel):
    question: str
    study_id: Optional[str] = None
    include_sql: bool = False


class ReportRequest(BaseModel):
    study_id: str
    site_id: str
    report_type: str = "full"


class NLQueryResponse(BaseModel):
    answer: str
    data: Optional[List[Dict]] = None
    sql_query: Optional[str] = None
    visualization_hint: Optional[str] = None


class InsightResponse(BaseModel):
    summary: str
    key_findings: List[str]
    recommendations: List[str]
    risk_areas: List[Dict[str, Any]]
    timestamp: str


# ============================================================================
# AI Functions
# ============================================================================

class ClinicalTrialAI:
    """AI-powered clinical trial analysis using Gemini"""
    
    def __init__(self):
        self.model = model
    
    def natural_language_to_sql(self, question: str, study_id: Optional[str] = None) -> str:
        """Convert natural language question to SQL query"""
        
        study_filter = f"Filter by study_id = '{study_id}'" if study_id else "No study filter (query all)"
        
        prompt = f"""
{SCHEMA_CONTEXT}

Convert this question to a SQL Server query. Return ONLY the SQL query, no explanations.
The query should be safe and read-only (SELECT only).
Limit results to 100 rows maximum.

{study_filter}

Question: {question}

Important:
- Use TOP 100 to limit results
- Use proper SQL Server syntax
- Only use tables/views mentioned in the schema above
- Return just the SQL, no markdown formatting

SQL Query:
"""
        
        response = self.model.generate_content(prompt)
        sql = response.text.strip()
        
        # Clean up the response
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        
        return sql.strip()
    
    def answer_question(self, question: str, data: pd.DataFrame) -> str:
        """Generate natural language answer from query results"""
        
        if data.empty:
            return "No data found matching your query."
        
        # Convert DataFrame to string representation
        if len(data) > 20:
            data_str = data.head(20).to_string()
            data_str += f"\n... and {len(data) - 20} more rows"
        else:
            data_str = data.to_string()
        
        prompt = f"""
Based on this clinical trial data:

{data_str}

Question: {question}

Provide a clear, concise answer in natural language. Include:
1. Direct answer to the question with specific numbers
2. Key insights or patterns observed
3. Any concerning trends (if applicable)

Keep the response professional and under 200 words.
"""
        
        response = self.model.generate_content(prompt)
        return response.text.strip()
    
    def generate_cra_report(self, study_id: str, site_id: str, site_data: Dict, subject_data: pd.DataFrame) -> str:
        """Generate AI-powered CRA monitoring report"""
        
        subject_summary = subject_data.describe().to_string() if not subject_data.empty else "No subject data available"
        
        prompt = f"""
{SCHEMA_CONTEXT}

Generate a professional CRA (Clinical Research Associate) monitoring report.

Study: {study_id}
Site: {site_id}

Site Metrics:
{json.dumps(site_data, indent=2, default=str)}

Subject Summary Statistics:
{subject_summary}

Create a comprehensive report with these sections:

## EXECUTIVE SUMMARY
(2-3 sentences overview)

## SITE PERFORMANCE METRICS
- Data Quality Index: [value] ([interpretation])
- Clean Subjects: [X] of [Y] ([percentage]%)
- Open Queries: [count]
- SDV Completion: [percentage]%

## KEY FINDINGS
(3-5 bullet points of important observations)

## AREAS REQUIRING ATTENTION
(List any concerns with priority level)

## RECOMMENDED ACTIONS
(Numbered list of specific actions with responsible parties)

## NEXT STEPS
(What should happen before next monitoring visit)

Use professional clinical trial terminology. Be specific with numbers.
"""
        
        response = self.model.generate_content(prompt)
        return response.text.strip()
    
    def get_site_recommendations(self, site_id: str, metrics: Dict) -> Dict:
        """Generate AI-powered recommendations for a site"""
        
        prompt = f"""
{SCHEMA_CONTEXT}

Analyze these metrics for site {site_id} and provide recommendations:

Metrics:
{json.dumps(metrics, indent=2, default=str)}

Provide a JSON response with this exact structure:
{{
    "risk_level": "CRITICAL|HIGH|MEDIUM|LOW",
    "risk_score": <number 0-100>,
    "summary": "<one sentence assessment>",
    "recommendations": [
        {{
            "priority": "HIGH|MEDIUM|LOW",
            "action": "<specific action to take>",
            "responsible_party": "<CRA|DM|Site|Safety|Investigator>",
            "expected_impact": "<what improvement to expect>",
            "timeline": "<when to complete>"
        }}
    ],
    "positive_observations": ["<list of things going well>"]
}}

Return ONLY valid JSON, no other text.
"""
        
        response = self.model.generate_content(prompt)
        
        try:
            text = response.text.strip()
            # Clean JSON from markdown
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            return json.loads(text)
        except:
            return {
                "risk_level": "MEDIUM",
                "risk_score": 50,
                "summary": "Unable to fully analyze. Manual review recommended.",
                "recommendations": [{
                    "priority": "HIGH",
                    "action": "Review site metrics manually and develop improvement plan",
                    "responsible_party": "CRA",
                    "expected_impact": "Identify specific improvement areas",
                    "timeline": "Within 1 week"
                }],
                "positive_observations": []
            }
    
    def generate_insights(self, study_id: Optional[str] = None) -> Dict:
        """Generate comprehensive data quality insights"""
        
        # Build query
        where_clause = f"WHERE study_id = '{study_id}'" if study_id else ""
        
        query = f"""
        SELECT 
            COUNT(DISTINCT subject_id) as total_subjects,
            AVG(data_quality_index) as avg_dqi,
            SUM(is_clean_patient) as clean_subjects,
            SUM(total_queries) as total_queries,
            SUM(safety_queries) as safety_queries,
            SUM(missing_visits) as missing_visits,
            SUM(uncoded_terms) as uncoded_terms,
            SUM(crfs_overdue_90) as critical_signatures,
            COUNT(DISTINCT site_id) as total_sites,
            COUNT(DISTINCT region) as total_regions
        FROM gold.fact_subject_metrics
        {where_clause}
        """
        
        try:
            data = execute_query(query)
            metrics = data.iloc[0].to_dict() if not data.empty else {}
        except:
            metrics = {}
        
        prompt = f"""
{SCHEMA_CONTEXT}

Analyze this clinical trial data quality summary and provide insights:

Metrics:
{json.dumps(metrics, indent=2, default=str)}

Study Filter: {study_id if study_id else 'All Studies'}

Provide a JSON response with this structure:
{{
    "overall_status": "EXCELLENT|GOOD|FAIR|NEEDS_ATTENTION|CRITICAL",
    "summary": "<2-3 sentence executive summary>",
    "key_findings": [
        "<finding 1>",
        "<finding 2>",
        "<finding 3>"
    ],
    "risk_areas": [
        {{"area": "<name>", "severity": "HIGH|MEDIUM|LOW", "description": "<details>"}},
    ],
    "recommendations": [
        "<actionable recommendation 1>",
        "<actionable recommendation 2>"
    ],
    "submission_readiness": {{
        "status": "READY|NEAR_READY|NOT_READY",
        "blockers": ["<list of blocking issues>"],
        "estimated_timeline": "<when could be ready>"
    }}
}}

Return ONLY valid JSON.
"""
        
        response = self.model.generate_content(prompt)
        
        try:
            text = response.text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            insights = json.loads(text)
        except:
            insights = {
                "overall_status": "UNKNOWN",
                "summary": "Unable to generate insights. Please check data availability.",
                "key_findings": [],
                "risk_areas": [],
                "recommendations": ["Review data manually"],
                "submission_readiness": {
                    "status": "UNKNOWN",
                    "blockers": ["Data analysis incomplete"],
                    "estimated_timeline": "TBD"
                }
            }
        
        insights["metrics"] = metrics
        insights["timestamp"] = datetime.now().isoformat()
        insights["study_id"] = study_id or "ALL"
        
        return insights


# Initialize AI instance
ai = ClinicalTrialAI()


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/")
async def root():
    """API Health Check"""
    return {
        "status": "healthy",
        "service": "Clinical Trial AI API",
        "version": "1.0.0",
        "model": "Gemini 1.5 Flash",
        "endpoints": {
            "ask": "POST /api/ask - Natural language queries",
            "report": "POST /api/generate-report - Generate CRA report",
            "recommendations": "GET /api/recommendations/{site_id} - Site recommendations",
            "insights": "GET /api/insights - Data quality insights"
        }
    }


@app.post("/api/ask", response_model=NLQueryResponse)
async def ask_question(request: NLQueryRequest):
    """
    Natural language query endpoint
    
    Example questions:
    - "Which sites have the lowest data quality?"
    - "How many subjects have missing visits?"
    - "Show me all critical safety queries"
    - "What is the SDV completion rate by region?"
    - "List the top 5 underperforming sites"
    """
    try:
        # Convert question to SQL
        sql_query = ai.natural_language_to_sql(request.question, request.study_id)
        
        # Execute query
        try:
            data = execute_query(sql_query)
            data_dict = data.to_dict(orient='records')
        except Exception as e:
            return NLQueryResponse(
                answer=f"I understood your question but encountered a query error: {str(e)}. Try rephrasing your question.",
                sql_query=sql_query if request.include_sql else None
            )
        
        # Generate natural language answer
        answer = ai.answer_question(request.question, data)
        
        # Determine visualization hint
        viz_hint = None
        question_lower = request.question.lower()
        if any(word in question_lower for word in ['compare', 'by region', 'by country', 'by site']):
            viz_hint = "bar_chart"
        elif any(word in question_lower for word in ['distribution', 'breakdown', 'percentage']):
            viz_hint = "pie_chart"
        elif any(word in question_lower for word in ['trend', 'over time', 'timeline']):
            viz_hint = "line_chart"
        elif any(word in question_lower for word in ['list', 'show', 'top', 'bottom']):
            viz_hint = "table"
        
        return NLQueryResponse(
            answer=answer,
            data=data_dict[:100] if data_dict else None,
            sql_query=sql_query if request.include_sql else None,
            visualization_hint=viz_hint
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI processing error: {str(e)}")


@app.post("/api/generate-report")
async def generate_report(request: ReportRequest):
    """
    Generate AI-powered CRA monitoring report for a specific site
    """
    try:
        # Get site metrics
        site_query = f"""
        SELECT * FROM gold.agg_site_performance 
        WHERE study_id = '{request.study_id}' AND site_id = '{request.site_id}'
        """
        
        subject_query = f"""
        SELECT TOP 50 * FROM gold.fact_subject_metrics 
        WHERE study_id = '{request.study_id}' AND site_id = '{request.site_id}'
        ORDER BY data_quality_index ASC
        """
        
        site_data = execute_query(site_query)
        subject_data = execute_query(subject_query)
        
        site_dict = site_data.iloc[0].to_dict() if not site_data.empty else {}
        
        # Generate report
        report = ai.generate_cra_report(
            request.study_id,
            request.site_id,
            site_dict,
            subject_data
        )
        
        return {
            "study_id": request.study_id,
            "site_id": request.site_id,
            "report_type": request.report_type,
            "generated_at": datetime.now().isoformat(),
            "report": report,
            "site_metrics": site_dict,
            "subject_count": len(subject_data)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation error: {str(e)}")


@app.get("/api/recommendations/{site_id}")
async def get_recommendations(
    site_id: str,
    study_id: Optional[str] = Query(None, description="Filter by study ID")
):
    """
    Get AI-powered recommendations for a specific site
    """
    try:
        where_clause = f"WHERE site_id = '{site_id}'"
        if study_id:
            where_clause += f" AND study_id = '{study_id}'"
        
        query = f"""
        SELECT 
            study_id, site_id, region, country,
            COUNT(DISTINCT subject_id) as total_subjects,
            AVG(data_quality_index) as avg_dqi,
            SUM(is_clean_patient) as clean_subjects,
            SUM(total_queries) as open_queries,
            SUM(safety_queries) as safety_queries,
            SUM(missing_visits) as missing_visits,
            SUM(crfs_overdue_90) as critical_signatures
        FROM gold.fact_subject_metrics
        {where_clause}
        GROUP BY study_id, site_id, region, country
        """
        
        data = execute_query(query)
        
        if data.empty:
            raise HTTPException(status_code=404, detail=f"Site {site_id} not found")
        
        metrics = data.iloc[0].to_dict()
        recommendations = ai.get_site_recommendations(site_id, metrics)
        
        return {
            "site_id": site_id,
            "study_id": study_id or "ALL",
            "metrics": metrics,
            "ai_analysis": recommendations,
            "generated_at": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recommendation error: {str(e)}")


@app.get("/api/insights")
async def get_insights(
    study_id: Optional[str] = Query(None, description="Filter by study ID")
):
    """
    Get comprehensive AI-powered data quality insights
    """
    try:
        insights = ai.generate_insights(study_id)
        return insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insights generation error: {str(e)}")


@app.get("/api/action-items")
async def get_action_items(
    study_id: Optional[str] = Query(None),
    priority: Optional[str] = Query(None, description="Filter by priority: Critical, High, Medium"),
    limit: int = Query(20, le=100)
):
    """
    Get prioritized action items with AI-enhanced descriptions
    """
    try:
        where_clauses = []
        if study_id:
            where_clauses.append(f"study_id = '{study_id}'")
        if priority:
            where_clauses.append(f"priority = '{priority}'")
        
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
        SELECT TOP {limit} * FROM gold.vw_action_items
        {where_sql}
        ORDER BY 
            CASE priority 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
                ELSE 4 
            END
        """
        
        data = execute_query(query)
        items = data.to_dict(orient='records')
        
        return {
            "total_items": len(items),
            "filters": {"study_id": study_id, "priority": priority},
            "action_items": items,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("  Clinical Trial AI API")
    print("  Powered by Gemini 1.5 Flash")
    print("=" * 60)
    print("\nStarting server...")
    print("API Documentation: http://localhost:8000/docs")
    print("Health Check: http://localhost:8000/")
    print("\nPress Ctrl+C to stop\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
