# CLAUDE.md — GlobalMart Data Intelligence Platform (Snowflake + dbt Edition)

> **Purpose:** This file is a complete, step-by-step build specification for replicating the GlobalMart Data Intelligence Platform on **Snowflake + dbt**. A coding agent (Claude) should be able to read this file and produce all the code, SQL, YAML, and configuration files needed to deploy the platform.

---

## PROJECT OVERVIEW

Rebuild the GlobalMart Data Intelligence Platform — originally built on Databricks (DLT + Unity Catalog + GenAI) — on a **Snowflake + dbt** stack with Snowflake Cortex for AI capabilities.

**Domain:** GlobalMart — a fictional US retail chain with 5 regions (East, West, South, Central, North) and 6 disconnected regional data systems.

**Three business failures this platform solves:**
1. **Revenue Audit** — 9% revenue overstatement from duplicate customers across regional systems
2. **Returns Fraud** — $2.3M annual loss from no cross-region visibility into return patterns
3. **Inventory Blindspot** — 12–18% revenue lost from products misallocated across regions

**Tech stack:** Snowflake, dbt-snowflake, Snowpipe, Snowflake Streams, Snowflake Tasks, Snowflake Cortex (COMPLETE, EMBED, Search Service), Cortex Analyst

---

## WHAT WAS BUILT ON DATABRICKS (Reference)

The original Databricks platform lives at `../` (parent directory). Key reference files:

| File | What It Contains |
|---|---|
| `../CLAUDE.md` | Full Databricks specification (DLT notebooks, quality rules, GenAI patterns) |
| `../documents/bronze_data_review.md` | All 27 source data issues found in Bronze |
| `../documents/quality_rules.md` | All 23 quality rules with business justifications |
| `../documents/dimensional_model_design.md` | Galaxy Schema design, relationship diagram, query walkthroughs |
| `../documents/reflection_answers.md` | GenAI prompt design, response parser, rules vs LLM |
| `../documents/snowflake_dbt_migration_flow.md` | Complete Databricks → Snowflake migration reference with SQL examples |
| `../notebooks/01_bronze_ingestion.py` | Databricks Bronze (Auto Loader) — reference for column names |
| `../notebooks/02_silver_harmonization.py` | Databricks Silver (quality rules) — reference for transformations |
| `../notebooks/03_gold_dimensional.py` | Databricks Gold (dimensional model) — reference for joins/logic |
| `../notebooks/04_metrics_aggregation.py` | Databricks Metrics — reference for aggregation SQL |
| `../notebooks/05_uc1_dq_reporter.py` through `08_uc4_executive_intel.py` | GenAI notebooks — reference for prompts/logic |
| `../data/` | Source data files (16 files across Region1-6 + root) |

**IMPORTANT:** Read `../documents/snowflake_dbt_migration_flow.md` first — it contains the full architecture mapping, all SQL examples, and detailed migration notes.

---

## SNOWFLAKE CONNECTION

```
SNOWFLAKE_ACCOUNT: Set via environment variable
SNOWFLAKE_USER:    Set via environment variable
SNOWFLAKE_PASSWORD: Set via environment variable
SNOWFLAKE_ROLE:    SYSADMIN
SNOWFLAKE_DATABASE: GLOBALMART
SNOWFLAKE_WAREHOUSE: GLOBALMART_TRANSFORM_WH
```

---

## PROJECT STRUCTURE TO CREATE

```
globalmart-snowflake/
├── CLAUDE.md                                    # THIS FILE — project specification
├── setup/
│   ├── 01_create_infrastructure.sql             # Database, schemas, warehouses, file formats
│   ├── 02_create_stage_and_upload.sql           # Internal stage + PUT commands
│   ├── 03_create_landing_tables.sql             # RAW_*_LOAD tables (VARIANT)
│   ├── 04_initial_seed.sql                      # COPY INTO for initial 16 files
│   ├── 05_create_snowpipes.sql                  # Snowpipe for continuous ingestion
│   ├── 06_create_streams_and_tasks.sql          # Streams + Tasks for auto-triggering dbt
│   └── 07_genai_cortex.sql                      # Cortex Search Service + GenAI output tables
├── globalmart_dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   │   ├── generate_rejected_rows.sql
│   │   └── test_in_range.sql
│   ├── models/
│   │   ├── sources.yml                          # RAW landing tables as sources
│   │   ├── bronze/
│   │   │   ├── _bronze__models.yml
│   │   │   ├── raw_customers.sql
│   │   │   ├── raw_orders.sql
│   │   │   ├── raw_transactions.sql
│   │   │   ├── raw_returns.sql
│   │   │   ├── raw_products.sql
│   │   │   └── raw_vendors.sql
│   │   ├── silver/
│   │   │   ├── _silver__models.yml              # 23 quality rules as dbt tests
│   │   │   ├── _customers_harmonized.sql        # Ephemeral: harmonize + flag (shared logic)
│   │   │   ├── _orders_harmonized.sql
│   │   │   ├── _transactions_harmonized.sql
│   │   │   ├── _returns_harmonized.sql
│   │   │   ├── _products_harmonized.sql
│   │   │   ├── _vendors_harmonized.sql
│   │   │   ├── clean_customers.sql              # Filters passes from harmonized
│   │   │   ├── clean_orders.sql
│   │   │   ├── clean_transactions.sql
│   │   │   ├── clean_returns.sql
│   │   │   ├── clean_products.sql
│   │   │   ├── clean_vendors.sql
│   │   │   ├── rejected_records.sql             # Filters failures from all harmonized
│   │   │   └── pipeline_audit_log.sql
│   │   ├── gold/
│   │   │   ├── _gold__models.yml
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_products.sql
│   │   │   ├── dim_vendors.sql
│   │   │   ├── dim_dates.sql
│   │   │   ├── fact_sales.sql
│   │   │   ├── fact_returns.sql
│   │   │   ├── bridge_return_products.sql
│   │   │   ├── mv_revenue_by_region.sql
│   │   │   ├── mv_return_rate_by_vendor.sql
│   │   │   └── mv_slow_moving_products.sql
│   │   ├── metrics/
│   │   │   ├── _metrics__models.yml
│   │   │   ├── mv_monthly_revenue_by_region.sql
│   │   │   ├── mv_segment_profitability.sql
│   │   │   ├── mv_customer_return_history.sql
│   │   │   ├── mv_return_reason_analysis.sql
│   │   │   ├── mv_vendor_product_performance.sql
│   │   │   └── mv_product_return_impact.sql
│   │   └── genai/
│   │       ├── _genai__models.yml
│   │       ├── dq_audit_report.sql
│   │       ├── flagged_return_customers.sql
│   │       ├── rag_query_history.py             # Snowpark Python model
│   │       └── ai_business_insights.sql
│   └── tests/
│       ├── assert_audit_log_balanced.sql
│       └── assert_bridge_allocation_sums.sql
├── cortex_analyst/
│   └── semantic_model.yaml                      # Cortex Analyst semantic model
└── deploy.sh                                    # Full deployment script
```

---

## EXECUTION ORDER

```
Step 1:  Run setup/01_create_infrastructure.sql    → Database, schemas, warehouses
Step 2:  Run setup/02_create_stage_and_upload.sql   → Stage + PUT 16 files
Step 3:  Run setup/03_create_landing_tables.sql     → 6 RAW_*_LOAD tables
Step 4:  Run setup/04_initial_seed.sql              → COPY INTO (one-time bulk load)
Step 5:  Run setup/05_create_snowpipes.sql          → 6 Snowpipes (continuous ingestion)
Step 6:  Run setup/06_create_streams_and_tasks.sql  → Streams + Tasks (auto-trigger dbt)
Step 7:  cd globalmart_dbt && dbt deps && dbt run --full-refresh
Step 8:  dbt test
Step 9:  Run setup/07_genai_cortex.sql              → Cortex Search + GenAI tables
Step 10: dbt run --select genai
Step 11: Upload cortex_analyst/semantic_model.yaml  → Set up Cortex Analyst
```

---

## STEP 1: INFRASTRUCTURE (setup/01_create_infrastructure.sql)

```sql
-- Database
CREATE DATABASE IF NOT EXISTS GLOBALMART;

-- Schemas (5 total)
CREATE SCHEMA IF NOT EXISTS GLOBALMART.RAW;        -- Staged files + landing tables
CREATE SCHEMA IF NOT EXISTS GLOBALMART.BRONZE;     -- dbt: raw extracted from VARIANT
CREATE SCHEMA IF NOT EXISTS GLOBALMART.SILVER;     -- dbt: cleaned + validated
CREATE SCHEMA IF NOT EXISTS GLOBALMART.GOLD;       -- dbt: dimensional model + GenAI outputs
CREATE SCHEMA IF NOT EXISTS GLOBALMART.METRICS;    -- dbt: pre-computed aggregation views

-- Warehouses (3 total, right-sized)
CREATE WAREHOUSE IF NOT EXISTS GLOBALMART_LOAD_WH
  WITH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE;

CREATE WAREHOUSE IF NOT EXISTS GLOBALMART_TRANSFORM_WH
  WITH WAREHOUSE_SIZE='SMALL' AUTO_SUSPEND=120 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE;

CREATE WAREHOUSE IF NOT EXISTS GLOBALMART_CORTEX_WH
  WITH WAREHOUSE_SIZE='MEDIUM' AUTO_SUSPEND=120 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE;

-- File formats
CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.CSV_FORMAT
  TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1
  NULL_IF=('','NULL','null') TRIM_SPACE=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;

CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.JSON_FORMAT
  TYPE='JSON' STRIP_OUTER_ARRAY=TRUE;
```

---

## STEP 2: STAGE + UPLOAD (setup/02_create_stage_and_upload.sql)

```sql
CREATE OR REPLACE STAGE GLOBALMART.RAW.SOURCE_DATA_STAGE
  COMMENT='Landing zone for GlobalMart regional source files';

-- PUT all 16 files (run from SnowSQL with access to data/ folder)
-- Region 1-6 files go into Region subfolders
-- Root files go into stage root
-- Use AUTO_COMPRESS=FALSE to keep files readable
-- See snowflake_dbt_migration_flow.md Section 2.6 for full PUT commands
```

**Source files to upload (16 total):**

| # | File | Location | Format |
|---|---|---|---|
| 1 | customers_1.csv | Region1/ | CSV |
| 2 | orders_1.csv | Region1/ | CSV |
| 3 | transactions_1.csv | Region1/ | CSV |
| 4 | customers_2.csv | Region2/ | CSV |
| 5 | transactions_1.csv | Region2/ | CSV |
| 6 | customers_3.csv | Region3/ | CSV |
| 7 | orders_2.csv | Region3/ | CSV |
| 8 | customers_4.csv | Region4/ | CSV |
| 9 | transactions_2.csv | Region4/ | CSV |
| 10 | customers_5.csv | Region5/ | CSV |
| 11 | orders_3.csv | Region5/ | CSV |
| 12 | customers_6.csv | Region6/ | CSV |
| 13 | returns_1.json | Region6/ | JSON |
| 14 | transactions_3.csv | root | CSV |
| 15 | returns_2.json | root | JSON |
| 16 | products.json | root | JSON |
| 17 | vendors.csv | root | CSV |

---

## STEP 3: LANDING TABLES (setup/03_create_landing_tables.sql)

Create 6 landing tables in `GLOBALMART.RAW`. Each table uses a single `VARIANT` column to handle schema evolution (different column names across regions).

```
RAW_CUSTOMERS_LOAD     (raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())
RAW_ORDERS_LOAD        (same 3 columns)
RAW_TRANSACTIONS_LOAD  (same 3 columns)
RAW_RETURNS_LOAD       (same 3 columns)
RAW_PRODUCTS_LOAD      (same 3 columns)
RAW_VENDORS_LOAD       (same 3 columns)
```

**Why VARIANT?** Source files have different column names per region (e.g., `customer_id` vs `CustomerID` vs `cust_id` vs `customer_identifier`). VARIANT stores each row as a JSON object — all column names preserved as keys, no fixed schema needed. This is the Snowflake equivalent of Databricks Auto Loader with `schemaEvolutionMode = addNewColumns`.

---

## STEP 4: INITIAL SEED (setup/04_initial_seed.sql)

One-time COPY INTO for the initial 16 files. Uses `OBJECT_CONSTRUCT_KEEP_NULL(*)` for CSV to pack all columns into VARIANT, and `$1` for JSON.

**Pattern matching eliminates the need for unions:** In Databricks, transactions and returns each needed TWO Auto Loader streams + `unionByName`. In Snowflake, a single `PATTERN => '.*transactions.*\\.csv'` matches files across all subdirectories automatically.

```
COPY INTO RAW_CUSTOMERS_LOAD     ... PATTERN => '.*customers.*\\.csv'       (6 files)
COPY INTO RAW_ORDERS_LOAD        ... PATTERN => '.*orders.*\\.csv'          (3 files)
COPY INTO RAW_TRANSACTIONS_LOAD  ... PATTERN => '.*transactions.*\\.csv'    (3 files)
COPY INTO RAW_RETURNS_LOAD       ... PATTERN => '.*returns.*\\.json'        (2 files)
COPY INTO RAW_PRODUCTS_LOAD      ... PATTERN => '.*products\\.json'         (1 file)
COPY INTO RAW_VENDORS_LOAD       ... PATTERN => '.*vendors\\.csv'           (1 file)
```

See `snowflake_dbt_migration_flow.md` Section 2.7 for complete SQL.

---

## STEP 5: SNOWPIPE — CONTINUOUS INGESTION (setup/05_create_snowpipes.sql)

Create 6 Snowpipes with `AUTO_INGEST = TRUE` — one per entity. These are the **primary ingestion method** for ongoing data. When a region drops a new file into cloud storage, Snowpipe loads it within seconds.

```
CUSTOMERS_PIPE       → RAW_CUSTOMERS_LOAD      PATTERN => '.*customers.*\\.csv'
ORDERS_PIPE          → RAW_ORDERS_LOAD         PATTERN => '.*orders.*\\.csv'
TRANSACTIONS_PIPE    → RAW_TRANSACTIONS_LOAD   PATTERN => '.*transactions.*\\.csv'
RETURNS_PIPE         → RAW_RETURNS_LOAD        PATTERN => '.*returns.*\\.json'
PRODUCTS_PIPE        → RAW_PRODUCTS_LOAD       PATTERN => '.*products\\.json'
VENDORS_PIPE         → RAW_VENDORS_LOAD        PATTERN => '.*vendors\\.csv'
```

**Requires:** Cloud event notification setup (S3 → SQS, Azure → Event Grid, GCS → Pub/Sub) pointing to the `notification_channel` from `SHOW PIPES`.

Snowpipe will NOT re-load files already loaded by COPY INTO (Step 4).

See `snowflake_dbt_migration_flow.md` Section 2.8 for complete SQL.

---

## STEP 6: STREAMS + TASKS (setup/06_create_streams_and_tasks.sql)

Create Streams on each RAW table to detect new rows. Create a Task that checks streams every 5 minutes and triggers `dbt run` when new data arrives.

```
CUSTOMERS_STREAM       ON RAW_CUSTOMERS_LOAD     APPEND_ONLY=TRUE
ORDERS_STREAM          ON RAW_ORDERS_LOAD        APPEND_ONLY=TRUE
TRANSACTIONS_STREAM    ON RAW_TRANSACTIONS_LOAD  APPEND_ONLY=TRUE
RETURNS_STREAM         ON RAW_RETURNS_LOAD       APPEND_ONLY=TRUE
PRODUCTS_STREAM        ON RAW_PRODUCTS_LOAD      APPEND_ONLY=TRUE
VENDORS_STREAM         ON RAW_VENDORS_LOAD       APPEND_ONLY=TRUE

TRANSFORM_ON_NEW_DATA  SCHEDULE='5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM')
    OR ...
  AS CALL RUN_DBT_PIPELINE();
```

---

## dbt PROJECT CONFIGURATION

### dbt_project.yml

```yaml
name: 'globalmart_dbt'
version: '1.0.0'
config-version: 2
profile: 'globalmart'

models:
  globalmart_dbt:
    bronze:
      +schema: BRONZE
      +materialized: incremental
      +incremental_strategy: append
    silver:
      +schema: SILVER
      +materialized: incremental
      +incremental_strategy: append
    gold:
      +schema: GOLD
      +materialized: table                # dims + MVs = full refresh
      # fact_sales, fact_returns, bridge → override to incremental at model level
    metrics:
      +schema: METRICS
      +materialized: view                 # recompute on read
    genai:
      +schema: GOLD
      +materialized: table                # LLM regenerates each run
```

### Materialization Rules

| Layer | Model | Materialization | Reason |
|---|---|---|---|
| Bronze | All `raw_*` | `incremental` (append) | Only process new RAW rows |
| Silver | All `clean_*` | `incremental` (append) | Only validate new Bronze rows |
| Silver | `rejected_records` | `incremental` (append) | Append new failures |
| Silver | `pipeline_audit_log` | `view` | Lightweight count query |
| Gold | `dim_customers` | `table` | ROW_NUMBER dedup needs full scan (374 rows) |
| Gold | `dim_products`, `dim_vendors` | `table` | Small reference data |
| Gold | `dim_dates` | `table` | Generated calendar |
| Gold | `fact_sales` | `incremental` (append) | High-volume, append new joins |
| Gold | `fact_returns` | `incremental` (append) | High-volume, append new joins |
| Gold | `bridge_return_products` | `incremental` (append) | Derived from new facts |
| Gold | `mv_*` (3 tables) | `table` | Aggregations need full scan |
| Metrics | All `mv_*` (6 views) | `view` | Zero cost until queried |
| GenAI | All 4 tables | `table` | LLM must regenerate |

### Incremental Pattern (used in ALL incremental models)

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT ...
FROM {{ ref('upstream_model') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

### profiles.yml

```yaml
globalmart:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: SYSADMIN
      database: GLOBALMART
      warehouse: GLOBALMART_TRANSFORM_WH
      schema: PUBLIC
      threads: 4
```

### sources.yml

```yaml
sources:
  - name: raw
    database: GLOBALMART
    schema: RAW
    tables:
      - name: RAW_CUSTOMERS_LOAD
      - name: RAW_ORDERS_LOAD
      - name: RAW_TRANSACTIONS_LOAD
      - name: RAW_RETURNS_LOAD
      - name: RAW_PRODUCTS_LOAD
      - name: RAW_VENDORS_LOAD
```

---

## BRONZE LAYER (6 incremental models)

Each Bronze model extracts typed columns from the VARIANT `raw_data` column. Zero transformations — just type casting and column extraction.

### raw_customers.sql — Extract all column name variants

```
raw_data:"customer_id"::STRING       AS customer_id
raw_data:"CustomerID"::STRING        AS customerid
raw_data:"cust_id"::STRING           AS cust_id
raw_data:"customer_identifier"::STRING AS customer_identifier
raw_data:"customer_name"::STRING     AS customer_name
raw_data:"full_name"::STRING         AS full_name
raw_data:"customer_email"::STRING    AS customer_email
raw_data:"email_address"::STRING     AS email_address
raw_data:"segment"::STRING           AS segment
raw_data:"customer_segment"::STRING  AS customer_segment
raw_data:"city"::STRING              AS city
raw_data:"state"::STRING             AS state
raw_data:"region"::STRING            AS region
_source_file
_load_timestamp
```

### raw_orders.sql

```
order_id, customer_id, vendor_id, ship_mode, order_status, order_purchase_date
_source_file, _load_timestamp
```

### raw_transactions.sql — Extract both casing variants

```
raw_data:"Order_id"::STRING   AS order_id_v1
raw_data:"Order_ID"::STRING   AS order_id_v2
raw_data:"Product_id"::STRING AS product_id_v1
raw_data:"Product_ID"::STRING AS product_id_v2
sales, quantity, discount, profit, payment_type
_source_file, _load_timestamp
```

### raw_returns.sql — Extract both JSON key variants

```
raw_data:"order_id"::STRING      AS order_id_v1
raw_data:"OrderId"::STRING       AS order_id_v2
raw_data:"return_reason"::STRING AS return_reason_v1
raw_data:"reason"::STRING        AS return_reason_v2
raw_data:"return_status"::STRING AS return_status_v1
raw_data:"status"::STRING        AS return_status_v2
raw_data:"refund_amount"::STRING AS refund_amount_v1
raw_data:"amount"::STRING        AS refund_amount_v2
raw_data:"return_date"::STRING   AS return_date_v1
raw_data:"date_of_return"::STRING AS return_date_v2
_source_file, _load_timestamp
```

### raw_products.sql

```
product_id, product_name, brand, category, upc
_source_file, _load_timestamp
```

### raw_vendors.sql

```
vendor_id, vendor_name
_source_file, _load_timestamp
```

---

## SILVER LAYER — HARMONIZATION + QUALITY (8 models)

### Architecture: Ephemeral Harmonized Models

To avoid duplicating transformation logic between `clean_*` and `rejected_records`, use **ephemeral** (CTE-only) models for harmonization:

```
_customers_harmonized.sql  (ephemeral) ──▶ clean_customers.sql (WHERE NOT _has_any_failure)
                                        └──▶ rejected_records.sql (WHERE _fail_* = TRUE)
```

Each `_*_harmonized` model:
1. Reads from Bronze
2. Applies all COALESCE, mapping, and parsing transformations
3. Computes `_fail_*` boolean flags for each quality rule
4. Computes `_has_any_failure` composite flag
5. Does NOT filter — returns all rows with flags attached

Then `clean_*` filters WHERE NOT `_has_any_failure`, and `rejected_records` explodes failures into quarantine rows.

### 27 Source Data Issues → Snowflake SQL Transformations

**Read `../documents/bronze_data_review.md` for the complete list of 27 issues.**

#### Customers Transformations:
- `COALESCE(customer_id, customerid, cust_id, customer_identifier)` → unified customer_id
- `COALESCE(customer_name, full_name)` → unified name
- `COALESCE(customer_email, email_address)` → unified email
- `COALESCE(segment, customer_segment)` → then map via CASE:
  - `CONS` → Consumer, `CORP` → Corporate, `HO` → Home Office, `COSUMER` → Consumer
- City/State swap for Region 4: `CASE WHEN _source_file ILIKE '%Region%4%' THEN state ELSE city END`
- Region mapping: `W` → West, `S` → South

#### Orders Transformations:
- Ship mode mapping: `1st Class` → First Class, `2nd Class` → Second Class, `Std Class` → Standard Class
- Status: `LOWER(TRIM(order_status))`
- Date parsing: `COALESCE(TRY_TO_TIMESTAMP(val, 'MM/DD/YYYY HH24:MI'), TRY_TO_TIMESTAMP(val, 'YYYY-MM-DD HH24:MI'), TRY_TO_TIMESTAMP(val, 'MM/DD/YYYY'), TRY_TO_TIMESTAMP(val, 'YYYY-MM-DD'))`

#### Transactions Transformations:
- `COALESCE(order_id_v1, order_id_v2)` → unified order_id
- `COALESCE(product_id_v1, product_id_v2)` → unified product_id
- Currency strip: `TRY_TO_DOUBLE(REGEXP_REPLACE(sales, '[^0-9.\\-]', ''))`
- Discount: `CASE WHEN discount ILIKE '%\\%%' THEN TRY_TO_DOUBLE(REPLACE(discount,'%',''))/100.0 ELSE TRY_TO_DOUBLE(discount) END`

#### Returns Transformations:
- COALESCE all 5 column pairs (order_id, reason, status, amount, date)
- Status mapping: `APPRVD` → Approved, `PENDG` → Pending, `RJCTD` → Rejected
- '?' handling: refund_amount '?' → NULL, return_reason '?' → 'Unknown' (then rejected)
- Date: `COALESCE(TRY_TO_DATE(val, 'YYYY-MM-DD'), TRY_TO_DATE(val, 'MM-DD-YYYY'), TRY_TO_DATE(val, 'MM/DD/YYYY'))`

#### Products Transformations:
- UPC scientific notation: `CAST(CAST(TRY_TO_DOUBLE(upc) AS BIGINT) AS VARCHAR)`
- TRIM on product_name, brand, category

#### Vendors Transformations:
- TRIM on vendor_id, vendor_name

### 23 Quality Rules (ALL must be implemented as _fail_* flags AND as dbt tests)

**Read `../documents/quality_rules.md` for business justifications.**

| # | Entity | Field | Rule | Issue Type | Snowflake Check |
|---|---|---|---|---|---|
| 1 | Customers | customer_id | NOT NULL after COALESCE | missing_value | `customer_id IS NULL` |
| 2 | Customers | customer_name | NOT NULL after COALESCE | missing_value | `customer_name IS NULL` |
| 3 | Customers | segment | Must be Consumer/Corporate/Home Office | invalid_value | `segment NOT IN ('Consumer','Corporate','Home Office')` |
| 4 | Customers | region | Must be East/West/South/Central/North | invalid_value | `region NOT IN ('East','West','South','Central','North')` |
| 5 | Orders | order_id | NOT NULL | missing_value | `order_id IS NULL` |
| 6 | Orders | customer_id | NOT NULL | missing_value | `customer_id IS NULL` |
| 7 | Orders | ship_mode | Must be First/Second/Standard Class or Same Day | invalid_value | `ship_mode NOT IN (...)` |
| 8 | Orders | order_status | Must be one of 7 valid statuses | invalid_value | `order_status NOT IN (...)` |
| 9 | Orders | order_purchase_date | Must be parseable | parse_failure | `order_purchase_date IS NULL AND _raw_order_date IS NOT NULL` |
| 10 | Transactions | order_id | NOT NULL after COALESCE | missing_value | `order_id IS NULL` |
| 11 | Transactions | product_id | NOT NULL after COALESCE | missing_value | `product_id IS NULL` |
| 12 | Transactions | sales | Must be >= 0 | out_of_range | `sales < 0` |
| 13 | Transactions | quantity | Must be > 0 | out_of_range | `quantity IS NULL OR quantity <= 0` |
| 14 | Transactions | discount | Must be in [0, 1] | out_of_range | `discount < 0 OR discount > 1` |
| 15 | Returns | order_id | NOT NULL after COALESCE | missing_value | `order_id IS NULL` |
| 16 | Returns | refund_amount | Must be >= 0 | out_of_range | `refund_amount < 0` |
| 17 | Returns | return_status | Must be Approved/Pending/Rejected | invalid_value | `return_status NOT IN (...)` |
| 18 | Returns | return_reason | Must not be NULL or 'Unknown' | invalid_value | `return_reason IS NULL OR return_reason = 'Unknown'` |
| 19 | Returns | return_date | Must be parseable | parse_failure | `return_date IS NULL AND _raw_return_date IS NOT NULL` |
| 20 | Products | product_id | NOT NULL | missing_value | `product_id IS NULL` |
| 21 | Products | product_name | NOT NULL | missing_value | `product_name IS NULL` |
| 22 | Vendors | vendor_id | NOT NULL after TRIM | missing_value | `vendor_id IS NULL` |
| 23 | Vendors | vendor_name | NOT NULL after TRIM | missing_value | `vendor_name IS NULL` |

### rejected_records Schema

```
source_table:   STRING   -- e.g., 'clean_customers'
record_key:     STRING   -- e.g., the customer_id value
affected_field: STRING   -- e.g., 'segment'
issue_type:     STRING   -- missing_value | invalid_value | out_of_range | parse_failure
record_data:    STRING   -- OBJECT_CONSTRUCT(*)::VARCHAR of the full row
_source_file:   STRING
rejected_at:    TIMESTAMP_NTZ
```

One row per failure per record (a record with 3 failures = 3 rows).

### pipeline_audit_log (view)

Reconciles counts per entity: `bronze_count = silver_count + rejected_count`

---

## GOLD LAYER — DIMENSIONAL MODEL (10 models)

**Read `../documents/dimensional_model_design.md` for the full Galaxy Schema design.**

### Galaxy Schema: Why Fact Constellation

Two fact tables (`fact_sales`, `fact_returns`) share four conformed dimensions (`dim_customers`, `dim_products`, `dim_vendors`, `dim_dates`). A `bridge_return_products` table resolves the many-to-many between order-level returns and line-item-level products.

### Dimensions (4 tables — full refresh)

**dim_customers** — ROW_NUMBER dedup: 748 raw → 374 unique
```sql
ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY _load_timestamp DESC,
    CASE WHEN customer_email IS NOT NULL THEN 0 ELSE 1 END
) AS row_num
-- Keep row_num = 1
```
Columns: customer_sk (surrogate via `dbt_utils.generate_surrogate_key`), customer_id, customer_name, customer_email, segment, city, state, region

**dim_products** — Direct from Silver. Columns: product_sk, product_id, product_name, brand, category, upc

**dim_vendors** — Direct from Silver. Columns: vendor_sk, vendor_id, vendor_name

**dim_dates** — Generated calendar using `GENERATOR(ROWCOUNT => 10000)` + `DATEADD`. Full years from min(order_date, return_date) to max. Columns: date_sk, date_key, year, quarter, month, month_name, day, day_of_week, is_weekend

### Facts (2 tables — incremental)

**fact_sales** — Grain: 1 per order line item
- Source: `clean_orders INNER JOIN clean_transactions ON order_id`
- FKs: customer_sk, product_sk, vendor_sk, order_date_sk
- Measures: sales, quantity, discount, profit
- Attributes: ship_mode, order_status, payment_type

**fact_returns** — Grain: 1 per return event
- Source: `clean_returns LEFT JOIN clean_orders ON order_id`
- LEFT JOIN preserves returns without matching orders
- FKs: customer_sk, vendor_sk, return_date_sk
- Measures: refund_amount
- Attributes: return_reason, return_status

### Bridge Table (1 — incremental)

**bridge_return_products** — Proportional refund allocation
- Source: `fact_returns INNER JOIN fact_sales ON order_id` (reads from Gold, not Silver)
- `allocation_weight = line_sales / NULLIF(total_order_sales, 0)`
- `allocated_refund = refund_amount * allocation_weight`
- Grain: 1 per return × product

### Gold Materialized Views (3 — full refresh)

**CRITICAL: These names are rubric-required. Do NOT rename.**

**mv_revenue_by_region** — year × month × region
- Measures: order_count, total_sales, total_profit, total_quantity, unique_customers

**mv_return_rate_by_vendor** — 1 per vendor
- Measures: total_orders, total_sales, return_order_count, total_refunded, return_rate_pct

**mv_slow_moving_products** — product × region
- `is_slow_moving = PERCENT_RANK() OVER (PARTITION BY region ORDER BY total_quantity) <= 0.25`
- Measures: total_sales, total_quantity, order_count, total_profit

---

## METRICS LAYER (6 views)

All read from Gold. All materialized as `view` (recompute on read).

| Table | Business Failure | Grain |
|---|---|---|
| `mv_monthly_revenue_by_region` | Revenue Audit | year × month × region |
| `mv_segment_profitability` | Revenue Audit | segment × region |
| `mv_customer_return_history` | Returns Fraud | 1 per customer |
| `mv_return_reason_analysis` | Returns Fraud | return_reason × region |
| `mv_vendor_product_performance` | Inventory Blindspot | vendor × product × region |
| `mv_product_return_impact` | Inventory Blindspot | product × region |

See `snowflake_dbt_migration_flow.md` Sections 8.1–8.6 for complete SQL.

---

## GenAI — SNOWFLAKE CORTEX (4 output tables)

### Key Difference from Databricks

| Databricks | Snowflake |
|---|---|
| `databricks-gpt-oss-20b` via OpenAI Python client | `SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', prompt)` in SQL |
| Custom `parse_llm_response()` needed (structured Python object) | **Not needed** — Cortex returns clean text |
| `ai_query()` + `get_json_object()` double-extraction | `CORTEX.COMPLETE()` returns text directly |
| `sentence-transformers` + FAISS (ephemeral) | Cortex Search Service (managed, persistent) |
| `all-MiniLM-L6-v2` local embeddings | `CORTEX.EMBED_TEXT_768('e5-base-v2', text)` |

### UC1: Data Quality Reporter → `dq_audit_report`

1. Read `rejected_records`, group by (entity, field, issue_type)
2. Count rejections, collect 3 sample records
3. Join `pipeline_audit_log` for total entity counts → compute rejection_rate_pct
4. Call `SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', prompt)` per group
5. Prompt must: forbid technical jargon, name specific field, identify business report at risk, recommend investigation

**Output schema:** entity, affected_field, issue_type, rejected_count, total_entity_records, rejection_rate_pct, sample_records, ai_explanation, generated_at

### UC2: Returns Fraud Investigator → `flagged_return_customers`

1. Read `mv_customer_return_history` for customer profiles
2. Calculate P75 thresholds
3. Apply 5 weighted rules (pure SQL, NOT LLM):
   - high_return_count (20): > P75 or > 3
   - high_total_refund (25): > P75
   - high_avg_refund (20): > P75
   - high_approval_rate (15): > 90% with ≥2 returns
   - multiple_reasons (20): ≥ 3 distinct reasons
4. Composite score (0-100). Flag if ≥ 40.
5. Call `CORTEX.COMPLETE()` ONLY for flagged customers
6. Brief must: cite numbers, acknowledge innocent explanation, identify strongest signal, recommend 2 actions

**Output schema:** customer_id, customer_name, segment, region, total_returns, total_refund_value, avg_refund_per_return, approval_rate, distinct_reason_count, return_reasons, anomaly_score, rules_violated, investigation_brief, generated_at

### UC3: Product Intelligence RAG → `rag_query_history`

1. Build document chunks from Gold/Metrics tables:
   - Product summaries (1 per product)
   - Product-region summaries (1 per product × region, includes slow-moving flag)
   - Vendor summaries (1 per vendor, includes return rate)
2. Store in `GLOBALMART.GOLD.RAG_DOCUMENTS` table
3. Create Cortex Search Service on the documents table
4. For each of 5 test questions: retrieve top-5 docs → `CORTEX.COMPLETE()` with context
5. LLM instruction: "Answer ONLY from retrieved documents. If not in documents, say so."

**5 Test Questions:**
1. "Which products are slow-moving and have low sales?"
2. "Which vendor has the highest return rate?"
3. "What are the top-selling products in the East region?"
4. "Which products have the highest return cost?"
5. "Which vendors are active in the West region and what are their sales?"

**Output schema:** question, answer, retrieved_documents, retrieved_count, top_distance, generated_at

### UC4: Executive Business Intelligence → `ai_business_insights`

1. Read pre-computed Metrics tables (NOT raw facts)
2. Compute summary KPIs (5-10 numbers, not 5000 rows)
3. Pass KPI summaries to `CORTEX.COMPLETE()` for 3 domain narratives:
   - `revenue_performance`: total revenue/profit, strongest/weakest regions, segment comparison
   - `vendor_return_rate`: highest return vendor, financial impact, renegotiation recommendations
   - `slow_moving_inventory`: total return cost, highest-cost products, reallocation recommendations
4. Demonstrate `CORTEX.COMPLETE()` in pure SQL (replaces Databricks `ai_query()`):
   - Revenue Assessment per Region — one-sentence health assessment
   - Vendor Risk Assessment — continue/renegotiate/drop recommendation

**Output schema:** insight_type, executive_summary, kpi_data (JSON), generated_at

---

## CORTEX ANALYST (Genie Space equivalent)

Create `cortex_analyst/semantic_model.yaml` with:
- All Gold dimension and fact tables defined with column descriptions
- All join relationships (6 FK relationships)
- Sample questions (3 minimum)
- Business context matching `../documents/genie_setup.md` Instructions tab text

Upload to a Snowflake stage and access via Cortex Analyst UI or REST API.

---

## TABLE COUNT VERIFICATION

| Layer | Count | Tables |
|---|---|---|
| RAW (landing) | 6 | RAW_*_LOAD |
| Bronze | 6 | raw_customers, raw_orders, raw_transactions, raw_returns, raw_products, raw_vendors |
| Silver | 8 | 6 clean_* + rejected_records + pipeline_audit_log |
| Gold | 10 | 4 dims + 2 facts + 1 bridge + 3 MVs |
| Metrics | 6 | 6 mv_* views |
| GenAI | 4 | dq_audit_report, flagged_return_customers, rag_query_history, ai_business_insights |
| **Total** | **40** | (6 RAW + 34 dbt models) |

---

## IMPORTANT RULES FOR THE CODING AGENT

1. **Read reference files before coding.** Always read `../documents/snowflake_dbt_migration_flow.md` for complete SQL examples. Read `../notebooks/*.py` for original Databricks logic when in doubt.

2. **Table names must match exactly.** `mv_revenue_by_region`, `mv_return_rate_by_vendor`, `mv_slow_moving_products` — these are rubric-required names.

3. **Quality rule count must be 23.** Consistent across all files.

4. **All Bronze/Silver models must be incremental** with `_load_timestamp` filter.

5. **Gold dims are full refresh, Gold facts are incremental.** See materialization table above.

6. **Cortex COMPLETE returns clean text.** No response parser needed (unlike Databricks).

7. **Use `TRY_TO_*` functions** for safe parsing: `TRY_TO_TIMESTAMP`, `TRY_TO_DOUBLE`, `TRY_TO_DATE`, `TRY_TO_NUMBER`.

8. **Use `dbt_utils.generate_surrogate_key`** for all surrogate keys.

9. **Ephemeral harmonized models** avoid duplicating transformation logic between clean_* and rejected_records.

10. **Test everything.** `dbt test` must validate all 23 quality rules pass on Silver clean tables.

11. **Source data lives at `../data/`** — the parent directory's data folder. Copy or symlink it for staging.

12. **Setup SQL scripts are idempotent** — use `IF NOT EXISTS`, `OR REPLACE` everywhere.

13. **Snowpipe is the primary ingestion** for continuous feeds. COPY INTO is only for initial seed.
