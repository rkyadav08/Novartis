# Novartis Clinical Trial Data & AI Insights

This repository combines:

1. A **FastAPI + Gemini** service for natural-language clinical data analysis (`ai-api/`).
2. A **layered SQL data warehouse** implementation (`dataWarehouse/`) with Bronze → Silver → Gold transformations and reporting procedures.

## Repository Structure

- `ai-api/aiapi.py` – FastAPI app, Gemini prompt logic, SQL execution, and AI-generated reporting/recommendations.
- `dataWarehouse/scripts/bronze/` – Bronze DDL and source-aligned stored procedures.
- `dataWarehouse/scripts/silver/` – Silver DDL and curated/cleansed stored procedures.
- `dataWarehouse/scripts/gold/` – Gold DDL and business-facing stored procedures (`dashboard_data`, `risk_score`, `priority_actions`, etc.).

## What This Solution Does

### AI API capabilities

The API currently supports:

- Converting natural language questions into SQL (`POST /api/ask`).
- Executing SQL and returning both tabular data and AI-written answers.
- Generating CRA-style monitoring reports (`POST /api/generate-report`).
- Producing site-level AI recommendations (`GET /api/recommendations/{site_id}`).
- Producing study-wide insights/readiness summaries (`GET /api/insights`).
- Returning prioritized action items (`GET /api/action-items`).

### Data warehouse capabilities

The SQL scripts implement a medallion-style warehouse pipeline:

- **Bronze:** ingestion and source-preserving procedure layer.
- **Silver:** standardized, cleaned, and harmonized procedure layer.
- **Gold:** analytics and dashboard-ready outputs for clinical operations.

## Setup

### 1) Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn pandas pydantic google-generativeai pyodbc
```

### 2) Environment variables

Set these before running the API:

```bash
export GEMINI_API_KEY="<your_gemini_api_key>"
export DB_SERVER="<sql_server_host>"
export DB_NAME="<database_name>"
export DB_USER="<db_user>"
export DB_PASSWORD="<db_password>"
```

> The API has mock-data fallback when `pyodbc` is unavailable, useful for local development.

### 3) Run the API

```bash
python ai-api/aiapi.py
```

- Swagger docs: `http://localhost:8000/docs`
- Health endpoint: `http://localhost:8000/`

## Example API Calls

### Natural language query

```bash
curl -X POST "http://localhost:8000/api/ask" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Which sites have the lowest data quality index?",
    "study_id": "STUDY001",
    "include_sql": true
  }'
```

### Generate report

```bash
curl -X POST "http://localhost:8000/api/generate-report" \
  -H "Content-Type: application/json" \
  -d '{
    "study_id": "STUDY001",
    "site_id": "SITE-001",
    "report_type": "full"
  }'
```
