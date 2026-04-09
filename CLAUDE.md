# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GlobalMart Data Intelligence Platform — a Snowflake + dbt rebuild of a Databricks platform for a fictional US retail chain with 6 disconnected regional data systems. The platform solves three business failures: revenue overstatement from duplicate customers (9%), returns fraud ($2.3M/year), and inventory misallocation (12-18% revenue loss).

**Tech stack:** Snowflake, dbt-snowflake, Snowpipe, Snowflake Streams/Tasks, Snowflake Cortex (COMPLETE, EMBED, Search Service), Cortex Analyst

## Architecture

```
Source Files (17) → Internal Stage → Snowpipe → RAW (VARIANT) → Stream → Task → dbt run
                                                   ↓
                                    BRONZE → SILVER → GOLD → METRICS → GenAI
```

**Five schemas:** RAW (landing VARIANT tables), BRONZE (typed extraction), SILVER (cleaned/validated), GOLD (dimensional model + GenAI outputs), METRICS (aggregation views)

**Warehouse:** COMPUTE_WH (existing — do not create new warehouses)

**Database:** GLOBALMART

## Key Specification Files

- `SNOWFLAKE_PROJECT_PLAN.md` — Complete build spec with all SQL, schemas, quality rules, and GenAI prompts. **Read this first.**
- `snowflake_dbt_migration_flow.md` — Databricks-to-Snowflake architecture mapping with full SQL examples for every component.
- `../documents/bronze_data_review.md` — All 27 source data issues found in Bronze (from original Databricks project)
- `../documents/quality_rules.md` — All 23 quality rules with business justifications
- `../documents/dimensional_model_design.md` — Galaxy Schema design and relationships
- `../notebooks/*.py` — Original Databricks notebooks (reference for transformation logic)

## Build & Deploy Commands

```bash
# Step 1-6: Run setup SQL scripts in order via SnowSQL
snowsql -f setup/01_create_infrastructure.sql
snowsql -f setup/02_create_stage_and_upload.sql
snowsql -f setup/03_create_landing_tables.sql
snowsql -f setup/04_initial_seed.sql
snowsql -f setup/05_create_snowpipes.sql
snowsql -f setup/06_create_streams_and_tasks.sql

# Step 7: dbt build
cd globalmart_dbt
dbt deps
dbt run --full-refresh

# Step 8: Run all tests (validates 23 quality rules on Silver clean tables)
dbt test

# Step 9: GenAI setup
snowsql -f setup/07_genai_cortex.sql

# Step 10: Run GenAI models only
dbt run --select genai

# Run a single model
dbt run --select model_name

# Run a model and its downstream dependents
dbt run --select model_name+
```

## Data Pipeline Layers (40 total objects: 6 RAW + 34 dbt models)

### Bronze (6 incremental models)
Extract typed columns from VARIANT `raw_data`. Zero transformations — just `raw_data:"column_name"::TYPE` casting. Multiple column name variants extracted via separate aliases (e.g., `customer_id`, `CustomerID`, `cust_id`, `customer_identifier`).

### Silver (8 models: 6 ephemeral harmonized + 6 clean + rejected_records + pipeline_audit_log)
- **Ephemeral `_*_harmonized` models** apply COALESCE, mapping, and parsing, then compute `_fail_*` boolean flags for each quality rule
- **`clean_*` models** filter `WHERE NOT _has_any_failure`
- **`rejected_records`** explodes failures into one quarantine row per failure per record
- **`pipeline_audit_log`** reconciles: `bronze_count = silver_count + rejected_count`
- All 23 quality rules must be implemented as both `_fail_*` flags in harmonized models AND as dbt tests in `_silver__models.yml`

### Gold (10 models — Galaxy Schema / Fact Constellation)
- **4 dimensions** (table, full refresh): `dim_customers` (ROW_NUMBER dedup: 748→374), `dim_products`, `dim_vendors`, `dim_dates` (generated calendar)
- **2 facts** (incremental append): `fact_sales` (grain: order line item), `fact_returns` (grain: return event, LEFT JOIN to orders)
- **1 bridge** (incremental): `bridge_return_products` — proportional refund allocation via `allocation_weight`
- **3 MVs** (table, full refresh): `mv_revenue_by_region`, `mv_return_rate_by_vendor`, `mv_slow_moving_products` — **names are rubric-required, do NOT rename**

### Metrics (6 views)
All materialized as `view`. Read from Gold tables.

### GenAI (4 table models)
All use `SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', prompt)`. Cortex returns clean text — no response parser needed.

## Critical Patterns

**Incremental filter** (all Bronze/Silver incremental models):
```sql
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

**Surrogate keys:** Always use `dbt_utils.generate_surrogate_key()`

**Safe parsing:** Always use `TRY_TO_*` functions (`TRY_TO_TIMESTAMP`, `TRY_TO_DOUBLE`, `TRY_TO_DATE`, `TRY_TO_NUMBER`)

**VARIANT extraction:** `raw_data:"key_name"::TYPE` (Snowflake semi-structured access)

**CSV to VARIANT:** `OBJECT_CONSTRUCT_KEEP_NULL(*)` packs all CSV columns into a single VARIANT. JSON uses `$1`.

**Setup SQL must be idempotent:** Use `IF NOT EXISTS` and `OR REPLACE` everywhere.

## Source Data (17 files in `data/`)

6 customer CSVs (Regions 1-6), 3 order CSVs (Regions 1,3,5), 3 transaction CSVs (Regions 2,4 + root), 2 return JSONs (Region 6 + root), 1 product JSON (root), 1 vendor CSV (root). Column names vary across regions — this is intentional and handled by VARIANT + COALESCE in Bronze/Silver.

## Snowflake Connection

```
Account:   chc70950.us-east-1
Token:     Stored in Connect-token-secret.txt (project root) — DO NOT commit this file
Role:      ACCOUNTADMIN
Database:  GLOBALMART
Warehouse: COMPUTE_WH
```
