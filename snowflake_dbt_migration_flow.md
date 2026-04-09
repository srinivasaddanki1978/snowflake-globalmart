# GlobalMart Data Intelligence Platform — Databricks to Snowflake + dbt Migration Flow

> **Purpose:** Complete migration reference mapping every Databricks component to its Snowflake + dbt equivalent. Contains full SQL examples, dbt model code, and architectural decisions for replicating the GlobalMart platform on Snowflake.

---

## 1. Architecture Mapping

| # | Databricks Component | Snowflake Equivalent | Notes |
|---|---|---|---|
| 1 | Unity Catalog (globalmart) | Snowflake Database `GLOBALMART` | Same logical container |
| 2 | Catalog Schemas (bronze, silver, gold, metrics) | Snowflake Schemas (RAW, BRONZE, SILVER, GOLD, METRICS) | Added RAW for landing tables |
| 3 | Managed Volume (`/Volumes/globalmart/bronze/source_data`) | Internal Stage `@GLOBALMART.RAW.SOURCE_DATA_STAGE` | PUT files to stage |
| 4 | Auto Loader (`cloudFiles`) | Snowpipe (`AUTO_INGEST=TRUE`) | Both auto-detect new files |
| 5 | `schemaEvolutionMode=addNewColumns` | VARIANT column + `OBJECT_CONSTRUCT_KEEP_NULL(*)` | Stores all columns as JSON keys |
| 6 | Delta Live Tables (DLT) | dbt-snowflake | Declarative transformation framework |
| 7 | DLT Streaming Tables | dbt incremental models (`append` strategy) | Process only new rows |
| 8 | DLT Materialized Views | dbt table/view models | Full refresh on each run |
| 9 | `@dlt.expect()` (quality tracking) | dbt tests in `schema.yml` | Quality monitoring |
| 10 | `_fail_*` boolean flags + `.filter()` | SQL CASE WHEN + WHERE NOT | Same pattern, SQL syntax |
| 11 | `@dlt.append_flow` (rejected_records) | dbt incremental model with UNION ALL | Append rejections from all entities |
| 12 | `dlt.read()` / `dlt.read_stream()` | `{{ ref('model_name') }}` / `{{ source('raw', 'table') }}` | dbt DAG references |
| 13 | `unionByName(allowMissingColumns=True)` | Single `PATTERN` regex in COPY INTO/Snowpipe | No unions needed in Snowflake |
| 14 | `databricks-gpt-oss-20b` (OpenAI client) | `SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', prompt)` | SQL-native, no Python client |
| 15 | Custom `parse_llm_response()` | **Not needed** — Cortex returns clean text | Major simplification |
| 16 | `ai_query()` + `get_json_object()` | `CORTEX.COMPLETE()` returns text directly | No double-extraction |
| 17 | `sentence-transformers` + FAISS (ephemeral) | Cortex Search Service (managed, persistent) | No rebuild on each run |
| 18 | `all-MiniLM-L6-v2` (local embeddings) | `CORTEX.EMBED_TEXT_768('e5-base-v2', text)` | Managed embedding API |
| 19 | Databricks Genie Space | Cortex Analyst + Semantic Model YAML | NL-to-SQL interface |
| 20 | Databricks Workflow (Job) | Snowflake Tasks DAG or dbt Cloud scheduling | Orchestration |
| 21 | `monotonically_increasing_id()` | `dbt_utils.generate_surrogate_key()` | Surrogate key generation |
| 22 | PySpark `Window` + `row_number()` | Snowflake `ROW_NUMBER() OVER (...)` | Same windowing pattern |
| 23 | `percentile_approx(col, 0.25)` | `PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY col)` | Approximate → exact |

---

## 2. Snowflake Infrastructure Setup

### 2.1 Database and Schemas

```sql
-- Database
CREATE DATABASE IF NOT EXISTS GLOBALMART;

-- Schemas (5 total)
CREATE SCHEMA IF NOT EXISTS GLOBALMART.RAW;       -- Staged files + VARIANT landing tables
CREATE SCHEMA IF NOT EXISTS GLOBALMART.BRONZE;    -- dbt: typed columns extracted from VARIANT
CREATE SCHEMA IF NOT EXISTS GLOBALMART.SILVER;    -- dbt: cleaned, validated, standardized
CREATE SCHEMA IF NOT EXISTS GLOBALMART.GOLD;      -- dbt: dimensional model + GenAI outputs
CREATE SCHEMA IF NOT EXISTS GLOBALMART.METRICS;   -- dbt: pre-computed aggregation views
```

**Why RAW?** Databricks uses a single Bronze schema for both raw ingestion and the Auto Loader checkpoint. In Snowflake, we separate the landing zone (RAW with VARIANT tables) from the typed extraction (BRONZE with dbt models). This gives a cleaner lineage: Stage → RAW → BRONZE → SILVER → GOLD → METRICS.

### 2.2 Warehouses

```sql
-- Loading warehouse (XS — file ingestion is I/O bound, not compute bound)
CREATE WAREHOUSE IF NOT EXISTS GLOBALMART_LOAD_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Transform warehouse (S — dbt transformations, joins, window functions)
CREATE WAREHOUSE IF NOT EXISTS GLOBALMART_TRANSFORM_WH
  WITH WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Cortex warehouse (M — LLM inference needs more compute)
CREATE WAREHOUSE IF NOT EXISTS GLOBALMART_CORTEX_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

### 2.3 File Formats

```sql
-- CSV format (customers, orders, transactions, vendors)
CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON format (returns, products)
CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;
```

### 2.4 Continuous Feed Strategy

The platform assumes **continuous data feeds** from 6 regional systems. The ingestion strategy has two phases:

1. **Initial Seed (one-time):** COPY INTO loads the 16 historical files already in the stage
2. **Primary Ingestion (ongoing):** Snowpipe with `AUTO_INGEST=TRUE` automatically loads new files as regions drop them into cloud storage

```
Regional System → Cloud Storage → Snowpipe (auto) → RAW_*_LOAD (VARIANT)
                                                          ↓
                                              Stream detects new rows
                                                          ↓
                                              Task triggers dbt run
                                                          ↓
                                    BRONZE → SILVER → GOLD → METRICS
```

### 2.5 Internal Stage

```sql
CREATE OR REPLACE STAGE GLOBALMART.RAW.SOURCE_DATA_STAGE
  FILE_FORMAT = GLOBALMART.RAW.CSV_FORMAT
  COMMENT = 'Landing zone for GlobalMart regional source files';
```

### 2.6 PUT Commands (Upload 16 Files)

Run from SnowSQL with access to the local `data/` folder:

```sql
-- Region 1 (3 files)
PUT file://data/Region1/customers_1.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region1/ AUTO_COMPRESS=FALSE;
PUT file://data/Region1/orders_1.csv    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region1/ AUTO_COMPRESS=FALSE;
PUT file://data/Region1/transactions_1.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region1/ AUTO_COMPRESS=FALSE;

-- Region 2 (2 files)
PUT file://data/Region2/customers_2.csv    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region2/ AUTO_COMPRESS=FALSE;
PUT file://data/Region2/transactions_1.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region2/ AUTO_COMPRESS=FALSE;

-- Region 3 (2 files)
PUT file://data/Region3/customers_3.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region3/ AUTO_COMPRESS=FALSE;
PUT file://data/Region3/orders_2.csv    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region3/ AUTO_COMPRESS=FALSE;

-- Region 4 (2 files)
PUT file://data/Region4/customers_4.csv    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region4/ AUTO_COMPRESS=FALSE;
PUT file://data/Region4/transactions_2.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region4/ AUTO_COMPRESS=FALSE;

-- Region 5 (2 files)
PUT file://data/Region5/customers_5.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region5/ AUTO_COMPRESS=FALSE;
PUT file://data/Region5/orders_3.csv    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region5/ AUTO_COMPRESS=FALSE;

-- Region 6 (2 files)
PUT file://data/Region6/customers_6.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region6/ AUTO_COMPRESS=FALSE;
PUT file://data/Region6/returns_1.json  @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region6/ AUTO_COMPRESS=FALSE;

-- Root-level files (4 files)
PUT file://data/transactions_3.csv @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
PUT file://data/returns_2.json     @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
PUT file://data/products.json      @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
PUT file://data/vendors.csv        @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
```

### 2.7 Landing Tables (6 VARIANT Tables)

Each landing table uses a single `VARIANT` column to handle schema evolution — different column names across regions are preserved as JSON keys.

```sql
-- Customers (6 CSV files, 4 different ID column names)
CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_CUSTOMERS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Orders (3 CSV files, date format mismatches)
CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_ORDERS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Transactions (3 CSV files split across regions + root)
CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Returns (2 JSON files split across Region6 + root)
CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_RETURNS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Products (1 JSON file)
CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_PRODUCTS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Vendors (1 CSV file)
CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_VENDORS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 2.8 COPY INTO — Initial Seed (One-Time)

```sql
-- Customers: 6 CSV files across Region1-6
COPY INTO GLOBALMART.RAW.RAW_CUSTOMERS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*customers.*\\.csv')
);

-- Orders: 3 CSV files across Region1, Region3, Region5
COPY INTO GLOBALMART.RAW.RAW_ORDERS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*orders.*\\.csv')
);

-- Transactions: 3 CSV files (Region1, Region2/Region4, root)
-- Single PATTERN matches ALL transaction files — no union needed
COPY INTO GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*transactions.*\\.csv')
);

-- Returns: 2 JSON files (Region6 + root)
-- Single PATTERN matches both — no union needed
COPY INTO GLOBALMART.RAW.RAW_RETURNS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        $1 AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.JSON_FORMAT', PATTERN => '.*returns.*\\.json')
);

-- Products: 1 JSON file
COPY INTO GLOBALMART.RAW.RAW_PRODUCTS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        $1 AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.JSON_FORMAT', PATTERN => '.*products\\.json')
);

-- Vendors: 1 CSV file
COPY INTO GLOBALMART.RAW.RAW_VENDORS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*vendors\\.csv')
);
```

**Key advantage over Databricks:** In Databricks, transactions and returns each required TWO Auto Loader streams + `unionByName(allowMissingColumns=True)` because files were split across subdirectories and root. In Snowflake, a single `PATTERN => '.*transactions.*\\.csv'` matches files across all subdirectories automatically. No unions needed.

### 2.9 Snowpipe — Continuous Ingestion (Primary)

Snowpipe is the **primary ingestion method** for ongoing data. When a region drops a new file into cloud storage, Snowpipe loads it within seconds — equivalent to Databricks Auto Loader.

```sql
-- Customers Pipe
CREATE OR REPLACE PIPE GLOBALMART.RAW.CUSTOMERS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO GLOBALMART.RAW.RAW_CUSTOMERS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*customers.*\\.csv')
);

-- Orders Pipe
CREATE OR REPLACE PIPE GLOBALMART.RAW.ORDERS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO GLOBALMART.RAW.RAW_ORDERS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*orders.*\\.csv')
);

-- Transactions Pipe
CREATE OR REPLACE PIPE GLOBALMART.RAW.TRANSACTIONS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*transactions.*\\.csv')
);

-- Returns Pipe
CREATE OR REPLACE PIPE GLOBALMART.RAW.RETURNS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO GLOBALMART.RAW.RAW_RETURNS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        $1 AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.JSON_FORMAT', PATTERN => '.*returns.*\\.json')
);

-- Products Pipe
CREATE OR REPLACE PIPE GLOBALMART.RAW.PRODUCTS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO GLOBALMART.RAW.RAW_PRODUCTS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        $1 AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.JSON_FORMAT', PATTERN => '.*products\\.json')
);

-- Vendors Pipe
CREATE OR REPLACE PIPE GLOBALMART.RAW.VENDORS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO GLOBALMART.RAW.RAW_VENDORS_LOAD (raw_data, _source_file, _load_timestamp)
FROM (
    SELECT
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS raw_data,
        METADATA$FILENAME AS _source_file,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @GLOBALMART.RAW.SOURCE_DATA_STAGE
    (FILE_FORMAT => 'GLOBALMART.RAW.CSV_FORMAT', PATTERN => '.*vendors\\.csv')
);
```

**Cloud Event Notification Setup:**
After creating pipes, run `SHOW PIPES` to get the `notification_channel` for each pipe, then configure:
- **AWS S3:** Create SQS queue → S3 event notification → point to Snowpipe's SQS ARN
- **Azure Blob:** Create Event Grid subscription → point to Snowpipe's notification channel
- **GCS:** Create Pub/Sub notification → point to Snowpipe's integration

```sql
-- Verify pipes and get notification channel
SHOW PIPES IN SCHEMA GLOBALMART.RAW;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('GLOBALMART.RAW.CUSTOMERS_PIPE');

-- Verify loaded files
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_CUSTOMERS_LOAD',
    START_TIME => DATEADD(HOURS, -24, CURRENT_TIMESTAMP())
));
```

**Important:** Snowpipe will NOT re-load files already loaded by COPY INTO (Step 4). It tracks file metadata internally.

### 2.10 Streams and Tasks — Auto-Trigger dbt

```sql
-- Create APPEND_ONLY streams on each RAW table
CREATE OR REPLACE STREAM GLOBALMART.RAW.CUSTOMERS_STREAM
  ON TABLE GLOBALMART.RAW.RAW_CUSTOMERS_LOAD APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM GLOBALMART.RAW.ORDERS_STREAM
  ON TABLE GLOBALMART.RAW.RAW_ORDERS_LOAD APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM GLOBALMART.RAW.TRANSACTIONS_STREAM
  ON TABLE GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM GLOBALMART.RAW.RETURNS_STREAM
  ON TABLE GLOBALMART.RAW.RAW_RETURNS_LOAD APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM GLOBALMART.RAW.PRODUCTS_STREAM
  ON TABLE GLOBALMART.RAW.RAW_PRODUCTS_LOAD APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM GLOBALMART.RAW.VENDORS_STREAM
  ON TABLE GLOBALMART.RAW.RAW_VENDORS_LOAD APPEND_ONLY = TRUE;

-- Task that checks streams every 5 minutes and triggers dbt
CREATE OR REPLACE TASK GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA
  WAREHOUSE = GLOBALMART_TRANSFORM_WH
  SCHEDULE = '5 MINUTE'
  WHEN
    SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.CUSTOMERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.ORDERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.TRANSACTIONS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.RETURNS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.PRODUCTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.VENDORS_STREAM')
AS
  -- Option A: Call dbt Cloud API
  -- CALL SYSTEM$SEND_NOTIFICATION('dbt_cloud_webhook', '{"cause": "new_data"}');
  -- Option B: Stored procedure that runs dbt via external function
  CALL RUN_DBT_PIPELINE();

-- Resume task (tasks are created suspended)
ALTER TASK GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA RESUME;
```

### 2.11 Data Flow Summary (Continuous Pipeline)

```
Step 1:  Region drops new file into cloud storage (e.g., S3 bucket)
Step 2:  Cloud event notification triggers Snowpipe
Step 3:  Snowpipe loads file → RAW_*_LOAD table (VARIANT row + _source_file + _load_timestamp)
Step 4:  Stream detects new rows in RAW table
Step 5:  Task fires (every 5 min when stream has data) → triggers dbt run
Step 6:  dbt Bronze models extract typed columns from VARIANT (incremental: new rows only)
Step 7:  dbt Silver models harmonize + validate (incremental: new Bronze rows only)
Step 8:  dbt Gold models build dimensions (full refresh) + facts (incremental: new Silver rows)
Step 9:  dbt Metrics views recompute on read (zero cost until queried)
Step 10: GenAI models regenerate (full refresh: Cortex COMPLETE re-runs)
Step 11: Cortex Analyst answers NL queries against updated Gold/Metrics tables
```

---

## 3. dbt Project Structure and Configuration

### 3.1 Full Directory Tree

```
globalmart_dbt/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── macros/
│   ├── generate_rejected_rows.sql
│   └── test_in_range.sql
├── models/
│   ├── sources.yml
│   ├── bronze/
│   │   ├── _bronze__models.yml
│   │   ├── raw_customers.sql
│   │   ├── raw_orders.sql
│   │   ├── raw_transactions.sql
│   │   ├── raw_returns.sql
│   │   ├── raw_products.sql
│   │   └── raw_vendors.sql
│   ├── silver/
│   │   ├── _silver__models.yml
│   │   ├── _customers_harmonized.sql        -- Ephemeral
│   │   ├── _orders_harmonized.sql           -- Ephemeral
│   │   ├── _transactions_harmonized.sql     -- Ephemeral
│   │   ├── _returns_harmonized.sql          -- Ephemeral
│   │   ├── _products_harmonized.sql         -- Ephemeral
│   │   ├── _vendors_harmonized.sql          -- Ephemeral
│   │   ├── clean_customers.sql
│   │   ├── clean_orders.sql
│   │   ├── clean_transactions.sql
│   │   ├── clean_returns.sql
│   │   ├── clean_products.sql
│   │   ├── clean_vendors.sql
│   │   ├── rejected_records.sql
│   │   └── pipeline_audit_log.sql
│   ├── gold/
│   │   ├── _gold__models.yml
│   │   ├── dim_customers.sql
│   │   ├── dim_products.sql
│   │   ├── dim_vendors.sql
│   │   ├── dim_dates.sql
│   │   ├── fact_sales.sql
│   │   ├── fact_returns.sql
│   │   ├── bridge_return_products.sql
│   │   ├── mv_revenue_by_region.sql
│   │   ├── mv_return_rate_by_vendor.sql
│   │   └── mv_slow_moving_products.sql
│   ├── metrics/
│   │   ├── _metrics__models.yml
│   │   ├── mv_monthly_revenue_by_region.sql
│   │   ├── mv_segment_profitability.sql
│   │   ├── mv_customer_return_history.sql
│   │   ├── mv_return_reason_analysis.sql
│   │   ├── mv_vendor_product_performance.sql
│   │   └── mv_product_return_impact.sql
│   └── genai/
│       ├── _genai__models.yml
│       ├── dq_audit_report.sql
│       ├── flagged_return_customers.sql
│       ├── rag_query_history.py              -- Snowpark Python model
│       └── ai_business_insights.sql
└── tests/
    ├── assert_audit_log_balanced.sql
    └── assert_bridge_allocation_sums.sql
```

### 3.2 dbt_project.yml

```yaml
name: 'globalmart_dbt'
version: '1.0.0'
config-version: 2
profile: 'globalmart'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

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
      +materialized: table
      # fact_sales, fact_returns, bridge_return_products override to incremental
    metrics:
      +schema: METRICS
      +materialized: view
    genai:
      +schema: GOLD
      +materialized: table
```

### 3.3 profiles.yml

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

### 3.4 packages.yml

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

### 3.5 sources.yml

```yaml
version: 2

sources:
  - name: raw
    database: GLOBALMART
    schema: RAW
    tables:
      - name: RAW_CUSTOMERS_LOAD
        description: "VARIANT landing table for 6 regional customer CSV files"
      - name: RAW_ORDERS_LOAD
        description: "VARIANT landing table for 3 regional order CSV files"
      - name: RAW_TRANSACTIONS_LOAD
        description: "VARIANT landing table for 3 transaction CSV files (regions + root)"
      - name: RAW_RETURNS_LOAD
        description: "VARIANT landing table for 2 return JSON files (Region6 + root)"
      - name: RAW_PRODUCTS_LOAD
        description: "VARIANT landing table for product catalog JSON"
      - name: RAW_VENDORS_LOAD
        description: "VARIANT landing table for vendor reference CSV"
```

### 3.6 Materialization Strategy

| Layer | Model | Materialization | Reason |
|---|---|---|---|
| Bronze | All `raw_*` | `incremental` (append) | Only process new RAW rows |
| Silver | `_*_harmonized` | `ephemeral` | CTE-only, shared by clean + rejected |
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
| GenAI | All 4 tables | `table` | LLM must regenerate each run |

### 3.7 Incremental Pattern (Used in ALL Incremental Models)

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT ...
FROM {{ ref('upstream_model') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

---

## 4. Source Data Issues — Databricks to Snowflake Fix Mapping

All 27 source data issues from `bronze_data_review.md` with their Snowflake SQL equivalents:

### 4.1 Customers (8 Issues)

| # | Issue | Databricks Fix | Snowflake Fix |
|---|---|---|---|
| 1 | 4 different ID columns: `customer_id`, `CustomerID`, `cust_id`, `customer_identifier` | `COALESCE(col("customer_id"), col("CustomerID"), col("cust_id"), col("customer_identifier"))` | `COALESCE(raw_data:"customer_id"::STRING, raw_data:"CustomerID"::STRING, raw_data:"cust_id"::STRING, raw_data:"customer_identifier"::STRING)` |
| 2 | 2 name columns: `customer_name` vs `full_name` | `COALESCE(col("customer_name"), col("full_name"))` | `COALESCE(raw_data:"customer_name"::STRING, raw_data:"full_name"::STRING)` |
| 3 | 2 email columns: `customer_email` vs `email_address` | `COALESCE(col("customer_email"), col("email_address"))` | `COALESCE(raw_data:"customer_email"::STRING, raw_data:"email_address"::STRING)` |
| 4 | 2 segment columns: `segment` vs `customer_segment` | `COALESCE(col("segment"), col("customer_segment"))` | `COALESCE(raw_data:"segment"::STRING, raw_data:"customer_segment"::STRING)` |
| 5 | City/state swap in Region 4 | `when(_source_file.contains("Region%204"), struct(state, city))` | `CASE WHEN _source_file ILIKE '%Region%4%' THEN state ELSE city END` |
| 6 | Segment abbreviations: CONS, CORP, HO | `when(segment=="CONS","Consumer").when(...)` | `CASE segment WHEN 'CONS' THEN 'Consumer' WHEN 'CORP' THEN 'Corporate' WHEN 'HO' THEN 'Home Office' ELSE segment END` |
| 7 | Segment typo: "Cosumer" | `when(segment=="Cosumer","Consumer")` | Include `WHEN 'Cosumer' THEN 'Consumer'` in CASE |
| 8 | Region abbreviations: W, S | `when(region=="W","West").when(region=="S","South")` | `CASE region WHEN 'W' THEN 'West' WHEN 'S' THEN 'South' ELSE region END` |

### 4.2 Orders (4 Issues)

| # | Issue | Databricks Fix | Snowflake Fix |
|---|---|---|---|
| 9 | Date format mismatch (MM/dd/yyyy HH:mm vs yyyy-MM-dd HH:mm) | `COALESCE(to_timestamp(val, fmt1), to_timestamp(val, fmt2), ...)` | `COALESCE(TRY_TO_TIMESTAMP(val, 'MM/DD/YYYY HH24:MI'), TRY_TO_TIMESTAMP(val, 'YYYY-MM-DD HH24:MI'), TRY_TO_TIMESTAMP(val, 'MM/DD/YYYY'), TRY_TO_TIMESTAMP(val, 'YYYY-MM-DD'))` |
| 10 | Ship mode abbreviations: 1st Class, 2nd Class, Std Class | `when(ship_mode.isin("1st","1st Class"),"First Class").when(...)` | `CASE WHEN ship_mode IN ('1st', '1st Class') THEN 'First Class' WHEN ship_mode IN ('2nd', '2nd Class') THEN 'Second Class' WHEN ship_mode IN ('Std', 'Std Class') THEN 'Standard Class' ELSE ship_mode END` |
| 11 | Inconsistent order status casing | `lower(trim(col("order_status")))` | `LOWER(TRIM(order_status))` |
| 12 | Unparseable dates → rejected | Route to `rejected_records` with `parse_failure` | Same: flag `_fail_unparseable_date` when all TRY_TO_TIMESTAMP return NULL but raw value was not NULL |

### 4.3 Transactions (5 Issues)

| # | Issue | Databricks Fix | Snowflake Fix |
|---|---|---|---|
| 13 | Column casing: `Order_id` vs `Order_ID` | `COALESCE(col("order_id"), col("Order_id"), col("Order_ID"))` | `COALESCE(raw_data:"Order_id"::STRING, raw_data:"Order_ID"::STRING)` (extracted in Bronze) |
| 14 | Product ID casing: `Product_id` vs `Product_ID` | `COALESCE(col("product_id"), col("Product_id"), col("Product_ID"))` | `COALESCE(raw_data:"Product_id"::STRING, raw_data:"Product_ID"::STRING)` |
| 15 | Discount as percentage strings: "20%" | `when(contains("%"), strip/divide by 100)` | `CASE WHEN discount_raw LIKE '%\\%%' THEN TRY_TO_DOUBLE(REPLACE(discount_raw, '%', '')) / 100.0 ELSE TRY_TO_DOUBLE(discount_raw) END` |
| 16 | Currency symbols in sales: "$150.00" | `regexp_replace("[^0-9.\\-]", "")` | `TRY_TO_DOUBLE(REGEXP_REPLACE(sales_raw, '[^0-9.\\-]', ''))` |
| 17 | Files split across Region folders + root | Two Auto Loader streams + `unionByName` | Single `PATTERN => '.*transactions.*\\.csv'` — no union needed |

### 4.4 Returns (6 Issues)

| # | Issue | Databricks Fix | Snowflake Fix |
|---|---|---|---|
| 18 | Every column name differs between files | `COALESCE` on all 5 column pairs | `COALESCE(raw_data:"order_id"::STRING, raw_data:"OrderId"::STRING)` etc. |
| 19 | Status abbreviations: APPRVD, PENDG, RJCTD | `when(status=="APPRVD","Approved").when(...)` | `CASE WHEN return_status IN ('APPRVD') THEN 'Approved' WHEN return_status IN ('PENDG') THEN 'Pending' WHEN return_status IN ('RJCTD') THEN 'Rejected' ELSE return_status END` |
| 20 | '?' values for unknown data | `when(val=="?", lit(None))` for amounts; `when(val=="?", "Unknown")` for reasons | `CASE WHEN refund_raw = '?' THEN NULL ELSE TRY_TO_DOUBLE(refund_raw) END` / `CASE WHEN reason_raw = '?' THEN 'Unknown' ELSE reason_raw END` |
| 21 | Date format mismatch | `COALESCE(to_timestamp(val, fmt1), ...)` | `COALESCE(TRY_TO_DATE(val, 'YYYY-MM-DD'), TRY_TO_DATE(val, 'MM-DD-YYYY'), TRY_TO_DATE(val, 'MM/DD/YYYY'))` |
| 22 | Files split across Region6 + root | Two Auto Loader streams + `unionByName` | Single `PATTERN => '.*returns.*\\.json'` — no union needed |
| 23 | JSON format (not CSV) | `cloudFiles.format = json` | `FILE_FORMAT => 'GLOBALMART.RAW.JSON_FORMAT'` + `$1` for VARIANT |

### 4.5 Products (2 Issues)

| # | Issue | Databricks Fix | Snowflake Fix |
|---|---|---|---|
| 24 | UPC in scientific notation (1.23456E+11) | `cast(double).cast(long).cast(string)` | `CAST(CAST(TRY_TO_DOUBLE(upc) AS BIGINT) AS VARCHAR)` |
| 25 | High NULL rates in optional columns | Preserve NULLs, only reject if PK null | Same: only `_fail_product_id_null` and `_fail_product_name_null` |

### 4.6 Vendors (2 Issues)

| # | Issue | Databricks Fix | Snowflake Fix |
|---|---|---|---|
| 26 | Leading/trailing whitespace | `trim()` on all string fields | `TRIM(vendor_id)`, `TRIM(vendor_name)` |
| 27 | Relatively clean data | Standard trim + NULL checks | Same |

---

## 5. Bronze Layer — dbt Models (6 Incremental)

Each Bronze model extracts typed columns from the VARIANT `raw_data` column. Zero transformations — just type casting and column naming.

### 5.1 raw_customers.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"customer_id"::STRING        AS customer_id,
    raw_data:"CustomerID"::STRING         AS customerid,
    raw_data:"cust_id"::STRING            AS cust_id,
    raw_data:"customer_identifier"::STRING AS customer_identifier,
    raw_data:"customer_name"::STRING      AS customer_name,
    raw_data:"full_name"::STRING          AS full_name,
    raw_data:"customer_email"::STRING     AS customer_email,
    raw_data:"email_address"::STRING      AS email_address,
    raw_data:"segment"::STRING            AS segment,
    raw_data:"customer_segment"::STRING   AS customer_segment,
    raw_data:"city"::STRING               AS city,
    raw_data:"state"::STRING              AS state,
    raw_data:"region"::STRING             AS region,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_CUSTOMERS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

### 5.2 raw_orders.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"order_id"::STRING            AS order_id,
    raw_data:"customer_id"::STRING         AS customer_id,
    raw_data:"vendor_id"::STRING           AS vendor_id,
    raw_data:"ship_mode"::STRING           AS ship_mode,
    raw_data:"order_status"::STRING        AS order_status,
    raw_data:"order_purchase_date"::STRING AS order_purchase_date,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_ORDERS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

### 5.3 raw_transactions.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"order_id"::STRING    AS order_id,
    raw_data:"Order_id"::STRING    AS order_id_v1,
    raw_data:"Order_ID"::STRING    AS order_id_v2,
    raw_data:"product_id"::STRING  AS product_id,
    raw_data:"Product_id"::STRING  AS product_id_v1,
    raw_data:"Product_ID"::STRING  AS product_id_v2,
    raw_data:"vendor_id"::STRING   AS vendor_id,
    raw_data:"sales"::STRING       AS sales,
    raw_data:"quantity"::STRING    AS quantity,
    raw_data:"discount"::STRING    AS discount,
    raw_data:"profit"::STRING      AS profit,
    raw_data:"payment_type"::STRING AS payment_type,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_TRANSACTIONS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

### 5.4 raw_returns.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"order_id"::STRING       AS order_id_v1,
    raw_data:"OrderId"::STRING        AS order_id_v2,
    raw_data:"return_reason"::STRING  AS return_reason_v1,
    raw_data:"reason"::STRING         AS return_reason_v2,
    raw_data:"return_status"::STRING  AS return_status_v1,
    raw_data:"status"::STRING         AS return_status_v2,
    raw_data:"refund_amount"::STRING  AS refund_amount_v1,
    raw_data:"RefundAmount"::STRING   AS refund_amount_v2,
    raw_data:"amount"::STRING         AS refund_amount_v3,
    raw_data:"return_date"::STRING    AS return_date_v1,
    raw_data:"ReturnDate"::STRING     AS return_date_v2,
    raw_data:"date_of_return"::STRING AS return_date_v3,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_RETURNS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

### 5.5 raw_products.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"product_id"::STRING   AS product_id,
    raw_data:"product_name"::STRING AS product_name,
    raw_data:"brand"::STRING        AS brand,
    raw_data:"category"::STRING     AS category,
    raw_data:"sub_category"::STRING AS sub_category,
    raw_data:"upc"::STRING          AS upc,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_PRODUCTS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

### 5.6 raw_vendors.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"vendor_id"::STRING   AS vendor_id,
    raw_data:"vendor_name"::STRING AS vendor_name,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_VENDORS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
```

---

## 6. Silver Layer — dbt Models (8 Models)

### 6.1 Architecture: Ephemeral Harmonized Models

To avoid duplicating transformation logic between `clean_*` and `rejected_records`, use **ephemeral** (CTE-only) models for harmonization:

```
_customers_harmonized.sql  (ephemeral) ──> clean_customers.sql     (WHERE NOT _has_any_failure)
                                       └──> rejected_records.sql   (WHERE _fail_* = TRUE, exploded)
```

Each `_*_harmonized` model:
1. Reads from Bronze (`{{ ref('raw_*') }}`)
2. Applies all COALESCE, mapping, and parsing transformations
3. Computes `_fail_*` boolean flags for each quality rule
4. Computes `_has_any_failure` composite flag
5. Does NOT filter — returns all rows with flags attached

Then `clean_*` filters WHERE NOT `_has_any_failure`, and `rejected_records` explodes failures into quarantine rows.

### 6.2 _customers_harmonized.sql (Ephemeral)

```sql
{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_customers') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ ref('clean_customers') }})
    {% endif %}
),

harmonized AS (
    SELECT
        -- COALESCE 4 ID column variants
        COALESCE(customer_id, customerid, cust_id, customer_identifier) AS customer_id,

        -- COALESCE name variants
        COALESCE(customer_name, full_name) AS customer_name,

        -- COALESCE email variants
        COALESCE(customer_email, email_address) AS customer_email,

        -- COALESCE + map segment
        CASE UPPER(TRIM(COALESCE(segment, customer_segment)))
            WHEN 'CONS' THEN 'Consumer'
            WHEN 'CORP' THEN 'Corporate'
            WHEN 'HO' THEN 'Home Office'
            WHEN 'COSUMER' THEN 'Consumer'
            WHEN 'CONSUMER' THEN 'Consumer'
            WHEN 'CORPORATE' THEN 'Corporate'
            WHEN 'HOME OFFICE' THEN 'Home Office'
            ELSE TRIM(COALESCE(segment, customer_segment))
        END AS segment,

        -- City/state swap for Region 4
        CASE
            WHEN _source_file ILIKE '%Region%4%' THEN state
            ELSE city
        END AS city,
        CASE
            WHEN _source_file ILIKE '%Region%4%' THEN city
            ELSE state
        END AS state,

        -- Map region abbreviations
        CASE UPPER(TRIM(region))
            WHEN 'W' THEN 'West'
            WHEN 'S' THEN 'South'
            WHEN 'EAST' THEN 'East'
            WHEN 'WEST' THEN 'West'
            WHEN 'SOUTH' THEN 'South'
            WHEN 'CENTRAL' THEN 'Central'
            WHEN 'NORTH' THEN 'North'
            ELSE TRIM(region)
        END AS region,

        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 1: customer_id NOT NULL
        (customer_id IS NULL) AS _fail_customer_id_null,
        -- Rule 2: customer_name NOT NULL
        (customer_name IS NULL) AS _fail_customer_name_null,
        -- Rule 3: valid segment
        (segment IS NULL OR segment NOT IN ('Consumer', 'Corporate', 'Home Office'))
            AS _fail_invalid_segment,
        -- Rule 4: valid region
        (region IS NULL OR region NOT IN ('East', 'West', 'South', 'Central', 'North'))
            AS _fail_invalid_region,

        -- Composite failure flag
        (customer_id IS NULL)
        OR (customer_name IS NULL)
        OR (segment IS NULL OR segment NOT IN ('Consumer', 'Corporate', 'Home Office'))
        OR (region IS NULL OR region NOT IN ('East', 'West', 'South', 'Central', 'North'))
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
```

### 6.3 _orders_harmonized.sql (Ephemeral)

```sql
{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_orders') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ ref('clean_orders') }})
    {% endif %}
),

harmonized AS (
    SELECT
        order_id,
        customer_id,
        vendor_id,

        -- Map ship mode abbreviations
        CASE
            WHEN TRIM(ship_mode) IN ('1st', '1st Class') THEN 'First Class'
            WHEN TRIM(ship_mode) IN ('2nd', '2nd Class') THEN 'Second Class'
            WHEN TRIM(ship_mode) IN ('Std', 'Std Class') THEN 'Standard Class'
            ELSE TRIM(ship_mode)
        END AS ship_mode,

        -- Normalize order status
        LOWER(TRIM(order_status)) AS order_status,

        -- Save raw date for rejection reporting
        order_purchase_date AS _raw_order_date,

        -- Parse dates with multiple format attempts
        COALESCE(
            TRY_TO_TIMESTAMP(order_purchase_date, 'MM/DD/YYYY HH24:MI'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'YYYY-MM-DD HH24:MI'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'MM/DD/YYYY'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'YYYY-MM-DD"T"HH24:MI:SS')
        ) AS order_purchase_date,

        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 5: order_id NOT NULL
        (order_id IS NULL) AS _fail_order_id_null,
        -- Rule 6: customer_id NOT NULL
        (customer_id IS NULL) AS _fail_customer_id_null,
        -- Rule 7: valid ship mode
        (ship_mode IS NULL OR ship_mode NOT IN ('First Class', 'Second Class', 'Standard Class', 'Same Day'))
            AS _fail_invalid_ship_mode,
        -- Rule 8: valid order status
        (order_status IS NULL OR order_status NOT IN
            ('canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable'))
            AS _fail_invalid_order_status,
        -- Rule 9: parseable date
        (order_purchase_date IS NULL AND _raw_order_date IS NOT NULL)
            AS _fail_unparseable_date,

        -- Composite
        (order_id IS NULL)
        OR (customer_id IS NULL)
        OR (ship_mode IS NULL OR ship_mode NOT IN ('First Class', 'Second Class', 'Standard Class', 'Same Day'))
        OR (order_status IS NULL OR order_status NOT IN
            ('canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable'))
        OR (order_purchase_date IS NULL AND _raw_order_date IS NOT NULL)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
```

### 6.4 _transactions_harmonized.sql (Ephemeral)

```sql
{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_transactions') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ ref('clean_transactions') }})
    {% endif %}
),

harmonized AS (
    SELECT
        -- COALESCE order_id variants
        COALESCE(order_id, order_id_v1, order_id_v2) AS order_id,

        -- COALESCE product_id variants
        COALESCE(product_id, product_id_v1, product_id_v2) AS product_id,

        vendor_id,

        -- Save raw values for rejection reporting
        sales AS _raw_sales,
        discount AS _raw_discount,

        -- Strip currency symbols from sales
        TRY_TO_DOUBLE(REGEXP_REPLACE(sales, '[^0-9.\\-]', '')) AS sales,

        -- Cast quantity
        TRY_TO_NUMBER(quantity) AS quantity,

        -- Convert percentage discounts
        CASE
            WHEN discount LIKE '%\\%%'
                THEN TRY_TO_DOUBLE(REPLACE(discount, '%', '')) / 100.0
            ELSE TRY_TO_DOUBLE(discount)
        END AS discount,

        TRY_TO_DOUBLE(profit) AS profit,
        payment_type,
        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 10: order_id NOT NULL
        (order_id IS NULL) AS _fail_order_id_null,
        -- Rule 11: product_id NOT NULL
        (product_id IS NULL) AS _fail_product_id_null,
        -- Rule 12: non-negative sales
        (sales IS NULL OR sales < 0) AS _fail_negative_sales,
        -- Rule 13: positive quantity
        (quantity IS NULL OR quantity <= 0) AS _fail_non_positive_quantity,
        -- Rule 14: discount in range 0-1
        (discount IS NULL OR discount < 0 OR discount > 1) AS _fail_discount_out_of_range,

        -- Composite
        (order_id IS NULL)
        OR (product_id IS NULL)
        OR (sales IS NULL OR sales < 0)
        OR (quantity IS NULL OR quantity <= 0)
        OR (discount IS NULL OR discount < 0 OR discount > 1)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
```

### 6.5 _returns_harmonized.sql (Ephemeral)

```sql
{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_returns') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ ref('clean_returns') }})
    {% endif %}
),

harmonized AS (
    SELECT
        -- COALESCE order_id variants
        COALESCE(order_id_v1, order_id_v2) AS order_id,

        -- COALESCE + map return reason ('?' → 'Unknown')
        CASE
            WHEN COALESCE(return_reason_v1, return_reason_v2) = '?' THEN 'Unknown'
            ELSE COALESCE(return_reason_v1, return_reason_v2)
        END AS return_reason,

        -- COALESCE + map return status abbreviations
        CASE UPPER(TRIM(COALESCE(return_status_v1, return_status_v2)))
            WHEN 'APPRVD' THEN 'Approved'
            WHEN 'PENDG' THEN 'Pending'
            WHEN 'RJCTD' THEN 'Rejected'
            WHEN 'APPROVED' THEN 'Approved'
            WHEN 'PENDING' THEN 'Pending'
            WHEN 'REJECTED' THEN 'Rejected'
            ELSE TRIM(COALESCE(return_status_v1, return_status_v2))
        END AS return_status,

        -- Save raw values for rejection reporting
        COALESCE(refund_amount_v1, refund_amount_v2, refund_amount_v3) AS _raw_refund,
        COALESCE(return_date_v1, return_date_v2, return_date_v3) AS _raw_return_date,

        -- Handle '?' refund amount → NULL
        CASE
            WHEN COALESCE(refund_amount_v1, refund_amount_v2, refund_amount_v3) = '?'
                THEN NULL
            ELSE TRY_TO_DOUBLE(COALESCE(refund_amount_v1, refund_amount_v2, refund_amount_v3))
        END AS refund_amount,

        -- Parse return date with multiple formats
        COALESCE(
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'YYYY-MM-DD'),
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'MM-DD-YYYY'),
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'MM/DD/YYYY'),
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'YYYY-MM-DD"T"HH24:MI:SS')
        ) AS return_date,

        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 15: order_id NOT NULL
        (order_id IS NULL) AS _fail_order_id_null,
        -- Rule 16: non-negative refund
        (refund_amount IS NULL AND _raw_refund != '?' AND _raw_refund IS NOT NULL)
            OR (refund_amount IS NOT NULL AND refund_amount < 0)
            AS _fail_negative_refund,
        -- Rule 17: valid return status
        (return_status IS NULL OR return_status NOT IN ('Approved', 'Pending', 'Rejected'))
            AS _fail_invalid_return_status,
        -- Rule 18: valid return reason (not NULL or Unknown)
        (return_reason IS NULL OR return_reason = 'Unknown')
            AS _fail_invalid_return_reason,
        -- Rule 19: parseable return date
        (return_date IS NULL AND _raw_return_date IS NOT NULL)
            AS _fail_unparseable_return_date,

        -- Composite
        (order_id IS NULL)
        OR ((refund_amount IS NULL AND _raw_refund != '?' AND _raw_refund IS NOT NULL)
            OR (refund_amount IS NOT NULL AND refund_amount < 0))
        OR (return_status IS NULL OR return_status NOT IN ('Approved', 'Pending', 'Rejected'))
        OR (return_reason IS NULL OR return_reason = 'Unknown')
        OR (return_date IS NULL AND _raw_return_date IS NOT NULL)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
```

### 6.6 _products_harmonized.sql (Ephemeral)

```sql
{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_products') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ ref('clean_products') }})
    {% endif %}
),

harmonized AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(brand) AS brand,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        -- Fix UPC scientific notation: 1.23456E+11 → "123456000000"
        CAST(CAST(TRY_TO_DOUBLE(upc) AS BIGINT) AS VARCHAR) AS upc,
        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 20: product_id NOT NULL
        (product_id IS NULL) AS _fail_product_id_null,
        -- Rule 21: product_name NOT NULL
        (product_name IS NULL) AS _fail_product_name_null,

        -- Composite
        (product_id IS NULL) OR (product_name IS NULL)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
```

### 6.7 _vendors_harmonized.sql (Ephemeral)

```sql
{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_vendors') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ ref('clean_vendors') }})
    {% endif %}
),

harmonized AS (
    SELECT
        TRIM(vendor_id) AS vendor_id,
        TRIM(vendor_name) AS vendor_name,
        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 22: vendor_id NOT NULL
        (vendor_id IS NULL OR vendor_id = '') AS _fail_vendor_id_null,
        -- Rule 23: vendor_name NOT NULL
        (vendor_name IS NULL OR vendor_name = '') AS _fail_vendor_name_null,

        -- Composite
        (vendor_id IS NULL OR vendor_id = '')
        OR (vendor_name IS NULL OR vendor_name = '')
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
```

### 6.8 clean_customers.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    customer_id,
    customer_name,
    customer_email,
    segment,
    city,
    state,
    region,
    _source_file,
    _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE NOT _has_any_failure
```

**Pattern repeats for all 6 clean_* models** — each reads from its harmonized ephemeral and filters `WHERE NOT _has_any_failure`, selecting only business columns.

### 6.9 rejected_records.sql

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

-- Customers rejections
SELECT
    'clean_customers' AS source_table,
    customer_id AS record_key,
    'customer_id' AS affected_field,
    'missing_value' AS issue_type,
    OBJECT_CONSTRUCT(*)::VARCHAR AS record_data,
    _source_file,
    CURRENT_TIMESTAMP() AS rejected_at,
    _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_customer_id_null

UNION ALL
SELECT 'clean_customers', customer_id, 'customer_name', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_customer_name_null

UNION ALL
SELECT 'clean_customers', customer_id, 'segment', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_invalid_segment

UNION ALL
SELECT 'clean_customers', customer_id, 'region', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_invalid_region

-- Orders rejections (5 rules)
UNION ALL
SELECT 'clean_orders', order_id, 'order_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_order_id_null

UNION ALL
SELECT 'clean_orders', order_id, 'customer_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_customer_id_null

UNION ALL
SELECT 'clean_orders', order_id, 'ship_mode', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_invalid_ship_mode

UNION ALL
SELECT 'clean_orders', order_id, 'order_status', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_invalid_order_status

UNION ALL
SELECT 'clean_orders', order_id, 'order_purchase_date', 'parse_failure',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_unparseable_date

-- Transactions rejections (5 rules)
UNION ALL
SELECT 'clean_transactions', order_id, 'order_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_order_id_null

UNION ALL
SELECT 'clean_transactions', order_id, 'product_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_product_id_null

UNION ALL
SELECT 'clean_transactions', order_id, 'sales', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_negative_sales

UNION ALL
SELECT 'clean_transactions', order_id, 'quantity', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_non_positive_quantity

UNION ALL
SELECT 'clean_transactions', order_id, 'discount', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_discount_out_of_range

-- Returns rejections (5 rules)
UNION ALL
SELECT 'clean_returns', order_id, 'order_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_order_id_null

UNION ALL
SELECT 'clean_returns', order_id, 'refund_amount', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_negative_refund

UNION ALL
SELECT 'clean_returns', order_id, 'return_status', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_invalid_return_status

UNION ALL
SELECT 'clean_returns', order_id, 'return_reason', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_invalid_return_reason

UNION ALL
SELECT 'clean_returns', order_id, 'return_date', 'parse_failure',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_unparseable_return_date

-- Products rejections (2 rules)
UNION ALL
SELECT 'clean_products', product_id, 'product_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_products_harmonized') }}
WHERE _fail_product_id_null

UNION ALL
SELECT 'clean_products', product_id, 'product_name', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_products_harmonized') }}
WHERE _fail_product_name_null

-- Vendors rejections (2 rules)
UNION ALL
SELECT 'clean_vendors', vendor_id, 'vendor_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_vendors_harmonized') }}
WHERE _fail_vendor_id_null

UNION ALL
SELECT 'clean_vendors', vendor_id, 'vendor_name', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_vendors_harmonized') }}
WHERE _fail_vendor_name_null
```

### 6.10 pipeline_audit_log.sql

```sql
{{ config(materialized='view') }}

SELECT 'customers' AS entity,
    (SELECT COUNT(*) FROM {{ ref('raw_customers') }}) AS bronze_count,
    (SELECT COUNT(*) FROM {{ ref('clean_customers') }}) AS silver_count,
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_customers') AS rejected_count
UNION ALL
SELECT 'orders',
    (SELECT COUNT(*) FROM {{ ref('raw_orders') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_orders') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_orders')
UNION ALL
SELECT 'transactions',
    (SELECT COUNT(*) FROM {{ ref('raw_transactions') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_transactions') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_transactions')
UNION ALL
SELECT 'returns',
    (SELECT COUNT(*) FROM {{ ref('raw_returns') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_returns') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_returns')
UNION ALL
SELECT 'products',
    (SELECT COUNT(*) FROM {{ ref('raw_products') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_products') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_products')
UNION ALL
SELECT 'vendors',
    (SELECT COUNT(*) FROM {{ ref('raw_vendors') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_vendors') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_vendors')
```

### 6.11 All 23 Quality Rules Summary

| # | Entity | Field | Rule | Issue Type | Snowflake SQL Check |
|---|---|---|---|---|---|
| 1 | Customers | customer_id | NOT NULL after COALESCE | missing_value | `customer_id IS NULL` |
| 2 | Customers | customer_name | NOT NULL after COALESCE | missing_value | `customer_name IS NULL` |
| 3 | Customers | segment | Valid set after mapping | invalid_value | `segment NOT IN ('Consumer','Corporate','Home Office')` |
| 4 | Customers | region | Valid set after mapping | invalid_value | `region NOT IN ('East','West','South','Central','North')` |
| 5 | Orders | order_id | NOT NULL | missing_value | `order_id IS NULL` |
| 6 | Orders | customer_id | NOT NULL | missing_value | `customer_id IS NULL` |
| 7 | Orders | ship_mode | Valid set after mapping | invalid_value | `ship_mode NOT IN ('First Class','Second Class','Standard Class','Same Day')` |
| 8 | Orders | order_status | Valid set (7 values) | invalid_value | `order_status NOT IN ('canceled','created',...)` |
| 9 | Orders | order_purchase_date | Parseable timestamp | parse_failure | `parsed IS NULL AND raw IS NOT NULL` |
| 10 | Transactions | order_id | NOT NULL after COALESCE | missing_value | `order_id IS NULL` |
| 11 | Transactions | product_id | NOT NULL after COALESCE | missing_value | `product_id IS NULL` |
| 12 | Transactions | sales | Non-negative after strip | out_of_range | `sales IS NULL OR sales < 0` |
| 13 | Transactions | quantity | Positive integer | out_of_range | `quantity IS NULL OR quantity <= 0` |
| 14 | Transactions | discount | Range 0-1 after convert | out_of_range | `discount < 0 OR discount > 1` |
| 15 | Returns | order_id | NOT NULL after COALESCE | missing_value | `order_id IS NULL` |
| 16 | Returns | refund_amount | Non-negative (handle '?') | out_of_range | `refund_amount < 0` |
| 17 | Returns | return_status | Valid set after mapping | invalid_value | `NOT IN ('Approved','Pending','Rejected')` |
| 18 | Returns | return_reason | Not NULL or Unknown | invalid_value | `IS NULL OR = 'Unknown'` |
| 19 | Returns | return_date | Parseable date | parse_failure | `parsed IS NULL AND raw IS NOT NULL` |
| 20 | Products | product_id | NOT NULL | missing_value | `product_id IS NULL` |
| 21 | Products | product_name | NOT NULL | missing_value | `product_name IS NULL` |
| 22 | Vendors | vendor_id | NOT NULL after TRIM | missing_value | `vendor_id IS NULL OR = ''` |
| 23 | Vendors | vendor_name | NOT NULL after TRIM | missing_value | `vendor_name IS NULL OR = ''` |

---

## 7. Gold Layer — dbt Models (10 Models)

### 7.1 Galaxy Schema Design

Two fact tables (`fact_sales`, `fact_returns`) share four conformed dimensions (`dim_customers`, `dim_products`, `dim_vendors`, `dim_dates`). A bridge table (`bridge_return_products`) resolves the many-to-many between order-level returns and line-item-level products.

```
                    ┌──────────────┐
                    │  dim_dates   │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
   ┌──────────────┐  ┌─────┴──────┐  ┌──────────────┐
   │dim_customers │──│ fact_sales  │──│ dim_products  │
   │(customer_id) │  │(order line) │  │(product_id)  │
   └──────┬───────┘  └─────┬──────┘  └──────┬───────┘
          │                │                 │
          │          ┌─────┴──────┐          │
          └──────────│fact_returns│          │
                     │(return evt)│          │
                     └─────┬──────┘          │
                           │                 │
                     ┌─────┴──────────┐      │
                     │bridge_return_  │──────┘
                     │   products     │
                     └────────────────┘
                           │
                     ┌─────┴──────┐
                     │dim_vendors │
                     │(vendor_id) │
                     └────────────┘
```

### 7.2 dim_customers.sql (Full Refresh — ROW_NUMBER Dedup)

```sql
{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                _load_timestamp DESC,
                CASE WHEN customer_email IS NOT NULL THEN 0 ELSE 1 END
        ) AS row_num
    FROM {{ ref('clean_customers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
    customer_id,
    customer_name,
    customer_email,
    segment,
    city,
    state,
    region
FROM ranked
WHERE row_num = 1
```

**748 raw → 374 unique customers after deduplication.**

### 7.3 dim_products.sql

```sql
{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    upc
FROM {{ ref('clean_products') }}
```

### 7.4 dim_vendors.sql

```sql
{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} AS vendor_sk,
    vendor_id,
    vendor_name
FROM {{ ref('clean_vendors') }}
```

### 7.5 dim_dates.sql (Generated Calendar)

```sql
{{ config(materialized='table') }}

WITH date_bounds AS (
    SELECT
        LEAST(
            (SELECT MIN(order_purchase_date)::DATE FROM {{ ref('clean_orders') }}),
            (SELECT MIN(return_date) FROM {{ ref('clean_returns') }})
        ) AS min_date,
        GREATEST(
            (SELECT MAX(order_purchase_date)::DATE FROM {{ ref('clean_orders') }}),
            (SELECT MAX(return_date) FROM {{ ref('clean_returns') }})
        ) AS max_date
),

calendar AS (
    SELECT
        DATEADD(DAY, seq4(), (SELECT DATE_TRUNC('YEAR', min_date) FROM date_bounds)) AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
    WHERE date_key <= (SELECT LAST_DAY(DATE_TRUNC('YEAR', max_date) + INTERVAL '1 YEAR' - INTERVAL '1 DAY') FROM date_bounds)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_key']) }} AS date_sk,
    date_key,
    YEAR(date_key) AS year,
    QUARTER(date_key) AS quarter,
    MONTH(date_key) AS month,
    MONTHNAME(date_key) AS month_name,
    DAY(date_key) AS day,
    DAYOFWEEK(date_key) AS day_of_week,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar
```

### 7.6 fact_sales.sql (Incremental)

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

WITH orders AS (
    SELECT * FROM {{ ref('clean_orders') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
    {% endif %}
),

transactions AS (
    SELECT * FROM {{ ref('clean_transactions') }}
),

customers AS (
    SELECT customer_sk, customer_id FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT product_sk, product_id FROM {{ ref('dim_products') }}
),

vendors AS (
    SELECT vendor_sk, vendor_id FROM {{ ref('dim_vendors') }}
),

dates AS (
    SELECT date_sk, date_key FROM {{ ref('dim_dates') }}
)

SELECT
    o.order_id,
    c.customer_sk,
    o.customer_id,
    p.product_sk,
    t.product_id,
    v.vendor_sk,
    COALESCE(o.vendor_id, t.vendor_id) AS vendor_id,
    d.date_sk AS order_date_sk,
    o.order_purchase_date::DATE AS order_date_key,
    t.sales,
    t.quantity,
    t.discount,
    t.profit,
    o.ship_mode,
    o.order_status,
    o.order_purchase_date,
    t.payment_type,
    o._load_timestamp
FROM orders o
INNER JOIN transactions t ON o.order_id = t.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON t.product_id = p.product_id
LEFT JOIN vendors v ON COALESCE(o.vendor_id, t.vendor_id) = v.vendor_id
LEFT JOIN dates d ON o.order_purchase_date::DATE = d.date_key
```

### 7.7 fact_returns.sql (Incremental)

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

WITH returns AS (
    SELECT * FROM {{ ref('clean_returns') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
    {% endif %}
),

orders AS (
    SELECT order_id, customer_id, vendor_id FROM {{ ref('clean_orders') }}
),

customers AS (
    SELECT customer_sk, customer_id FROM {{ ref('dim_customers') }}
),

dates AS (
    SELECT date_sk, date_key FROM {{ ref('dim_dates') }}
)

SELECT
    r.order_id,
    c.customer_sk,
    COALESCE(o.customer_id, r.order_id) AS customer_id,
    d.date_sk AS return_date_sk,
    r.return_date AS return_date_key,
    r.refund_amount,
    r.return_reason,
    r.return_status,
    r.return_date,
    o.vendor_id,
    r._load_timestamp
FROM returns r
LEFT JOIN orders o ON r.order_id = o.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN dates d ON r.return_date = d.date_key
```

### 7.8 bridge_return_products.sql (Incremental)

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

WITH returns AS (
    SELECT * FROM {{ ref('fact_returns') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
    {% endif %}
),

sales AS (
    SELECT * FROM {{ ref('fact_sales') }}
),

order_totals AS (
    SELECT
        order_id,
        SUM(sales) AS total_order_sales
    FROM sales
    GROUP BY order_id
),

joined AS (
    SELECT
        r.order_id,
        s.product_sk,
        s.product_id,
        r.return_date_sk,
        r.return_date_key,
        r.return_reason,
        r.return_status,
        r.refund_amount,
        s.sales AS line_sales,
        ot.total_order_sales,
        s.sales / NULLIF(ot.total_order_sales, 0) AS allocation_weight,
        r.refund_amount * (s.sales / NULLIF(ot.total_order_sales, 0)) AS allocated_refund,
        r._load_timestamp
    FROM returns r
    INNER JOIN sales s ON r.order_id = s.order_id
    INNER JOIN order_totals ot ON r.order_id = ot.order_id
)

SELECT
    order_id,
    product_sk,
    product_id,
    return_date_sk,
    return_date_key,
    return_reason,
    return_status,
    refund_amount,
    line_sales,
    total_order_sales,
    ROUND(allocation_weight, 4) AS allocation_weight,
    ROUND(allocated_refund, 2) AS allocated_refund,
    _load_timestamp
FROM joined
```

### 7.9 mv_revenue_by_region.sql

```sql
{{ config(materialized='table') }}

SELECT
    d.year,
    d.month,
    d.month_name,
    c.region,
    COUNT(DISTINCT s.order_id) AS order_count,
    ROUND(SUM(s.sales), 2) AS total_sales,
    ROUND(SUM(s.profit), 2) AS total_profit,
    SUM(s.quantity) AS total_quantity,
    COUNT(DISTINCT s.customer_id) AS unique_customers
FROM {{ ref('fact_sales') }} s
INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
INNER JOIN {{ ref('dim_dates') }} d ON s.order_date_sk = d.date_sk
GROUP BY d.year, d.month, d.month_name, c.region
```

### 7.10 mv_return_rate_by_vendor.sql

```sql
{{ config(materialized='table') }}

WITH vendor_sales AS (
    SELECT
        vendor_id,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(sales), 2) AS total_sales
    FROM {{ ref('fact_sales') }}
    WHERE vendor_id IS NOT NULL
    GROUP BY vendor_id
),

vendor_returns AS (
    SELECT
        vendor_id,
        COUNT(DISTINCT order_id) AS return_order_count,
        ROUND(SUM(refund_amount), 2) AS total_refunded
    FROM {{ ref('fact_returns') }}
    WHERE vendor_id IS NOT NULL
    GROUP BY vendor_id
)

SELECT
    vs.vendor_id,
    v.vendor_name,
    vs.total_orders,
    vs.total_sales,
    COALESCE(vr.return_order_count, 0) AS return_order_count,
    COALESCE(vr.total_refunded, 0.0) AS total_refunded,
    ROUND(COALESCE(vr.return_order_count, 0) * 100.0 / NULLIF(vs.total_orders, 0), 2) AS return_rate_pct
FROM vendor_sales vs
INNER JOIN {{ ref('dim_vendors') }} v ON vs.vendor_id = v.vendor_id
LEFT JOIN vendor_returns vr ON vs.vendor_id = vr.vendor_id
```

### 7.11 mv_slow_moving_products.sql

```sql
{{ config(materialized='table') }}

WITH product_region_sales AS (
    SELECT
        s.product_id,
        p.product_name,
        p.brand,
        c.region,
        ROUND(SUM(s.sales), 2) AS total_sales,
        SUM(s.quantity) AS total_quantity,
        COUNT(DISTINCT s.order_id) AS order_count,
        ROUND(SUM(s.profit), 2) AS total_profit
    FROM {{ ref('fact_sales') }} s
    INNER JOIN {{ ref('dim_products') }} p ON s.product_sk = p.product_sk
    INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
    GROUP BY s.product_id, p.product_name, p.brand, c.region
)

SELECT
    *,
    CASE
        WHEN PERCENT_RANK() OVER (PARTITION BY region ORDER BY total_quantity) <= 0.25
        THEN TRUE ELSE FALSE
    END AS is_slow_moving
FROM product_region_sales
```

---

## 8. Metrics Layer — dbt Models (6 Views)

### 8.1 mv_monthly_revenue_by_region.sql

```sql
{{ config(materialized='view') }}

SELECT
    d.year,
    d.month,
    d.month_name,
    c.region,
    COUNT(DISTINCT s.order_id) AS order_count,
    ROUND(SUM(s.sales), 2) AS total_revenue,
    ROUND(SUM(s.profit), 2) AS total_profit,
    SUM(s.quantity) AS total_quantity,
    COUNT(DISTINCT s.customer_id) AS unique_customers,
    ROUND(AVG(s.sales), 2) AS avg_order_value
FROM {{ ref('fact_sales') }} s
INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
INNER JOIN {{ ref('dim_dates') }} d ON s.order_date_sk = d.date_sk
GROUP BY d.year, d.month, d.month_name, c.region
```

### 8.2 mv_segment_profitability.sql

```sql
{{ config(materialized='view') }}

SELECT
    c.segment,
    c.region,
    COUNT(DISTINCT s.order_id) AS order_count,
    COUNT(DISTINCT s.customer_id) AS customer_count,
    ROUND(SUM(s.sales), 2) AS total_revenue,
    ROUND(SUM(s.profit), 2) AS total_profit,
    ROUND(SUM(s.profit) * 100.0 / NULLIF(SUM(s.sales), 0), 2) AS profit_margin_pct,
    ROUND(AVG(s.discount), 4) AS avg_discount
FROM {{ ref('fact_sales') }} s
INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
GROUP BY c.segment, c.region
```

### 8.3 mv_customer_return_history.sql

```sql
{{ config(materialized='view') }}

SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    c.region,
    COUNT(*) AS total_returns,
    ROUND(SUM(r.refund_amount), 2) AS total_refund_value,
    ROUND(AVG(r.refund_amount), 2) AS avg_refund_per_return,
    COUNT(DISTINCT r.return_reason) AS distinct_reason_count,
    LISTAGG(DISTINCT r.return_reason, ', ') WITHIN GROUP (ORDER BY r.return_reason) AS return_reasons,
    SUM(CASE WHEN r.return_status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    ROUND(SUM(CASE WHEN r.return_status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS approval_rate_pct,
    MIN(r.return_date) AS first_return_date,
    MAX(r.return_date) AS last_return_date
FROM {{ ref('fact_returns') }} r
INNER JOIN {{ ref('dim_customers') }} c ON r.customer_sk = c.customer_sk
WHERE r.customer_sk IS NOT NULL
GROUP BY c.customer_id, c.customer_name, c.segment, c.region
```

### 8.4 mv_return_reason_analysis.sql

```sql
{{ config(materialized='view') }}

SELECT
    r.return_reason,
    c.region,
    COUNT(*) AS return_count,
    ROUND(SUM(r.refund_amount), 2) AS total_refunded,
    ROUND(AVG(r.refund_amount), 2) AS avg_refund,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    SUM(CASE WHEN r.return_status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN r.return_status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_count
FROM {{ ref('fact_returns') }} r
INNER JOIN {{ ref('dim_customers') }} c ON r.customer_sk = c.customer_sk
WHERE r.customer_sk IS NOT NULL
GROUP BY r.return_reason, c.region
```

### 8.5 mv_vendor_product_performance.sql

```sql
{{ config(materialized='view') }}

WITH sales_agg AS (
    SELECT
        v.vendor_id,
        v.vendor_name,
        p.product_id,
        p.product_name,
        p.brand,
        c.region,
        COUNT(DISTINCT s.order_id) AS order_count,
        ROUND(SUM(s.sales), 2) AS total_sales,
        SUM(s.quantity) AS total_quantity,
        ROUND(SUM(s.profit), 2) AS total_profit
    FROM {{ ref('fact_sales') }} s
    INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
    INNER JOIN {{ ref('dim_products') }} p ON s.product_sk = p.product_sk
    INNER JOIN {{ ref('dim_vendors') }} v ON s.vendor_sk = v.vendor_sk
    GROUP BY v.vendor_id, v.vendor_name, p.product_id, p.product_name, p.brand, c.region
),

return_agg AS (
    SELECT
        s.vendor_id,
        b.product_id,
        COUNT(*) AS return_count,
        ROUND(SUM(b.allocated_refund), 2) AS total_allocated_refund
    FROM {{ ref('bridge_return_products') }} b
    INNER JOIN (
        SELECT DISTINCT order_id, vendor_id
        FROM {{ ref('fact_sales') }}
    ) s ON b.order_id = s.order_id
    GROUP BY s.vendor_id, b.product_id
)

SELECT
    sa.vendor_id,
    sa.vendor_name,
    sa.product_id,
    sa.product_name,
    sa.brand,
    sa.region,
    sa.order_count,
    sa.total_sales,
    sa.total_quantity,
    sa.total_profit,
    COALESCE(ra.return_count, 0) AS return_count,
    COALESCE(ra.total_allocated_refund, 0.0) AS total_allocated_refund
FROM sales_agg sa
LEFT JOIN return_agg ra ON sa.vendor_id = ra.vendor_id AND sa.product_id = ra.product_id
```

### 8.6 mv_product_return_impact.sql

```sql
{{ config(materialized='view') }}

WITH sales_with_region AS (
    SELECT DISTINCT
        s.order_id,
        s.product_sk,
        c.region
    FROM {{ ref('fact_sales') }} s
    INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
)

SELECT
    p.product_id,
    p.product_name,
    p.brand,
    sr.region,
    COUNT(*) AS return_line_count,
    ROUND(SUM(b.allocated_refund), 2) AS total_return_cost,
    ROUND(AVG(b.allocated_refund), 2) AS avg_return_cost,
    ROUND(SUM(b.allocation_weight), 4) AS total_allocation_weight
FROM {{ ref('bridge_return_products') }} b
INNER JOIN sales_with_region sr ON b.order_id = sr.order_id AND b.product_sk = sr.product_sk
INNER JOIN {{ ref('dim_products') }} p ON b.product_sk = p.product_sk
GROUP BY p.product_id, p.product_name, p.brand, sr.region
```

---

## 9. GenAI Migration — Snowflake Cortex (4 Output Tables)

### 9.1 Key Differences from Databricks

| Aspect | Databricks | Snowflake |
|---|---|---|
| LLM Model | `databricks-gpt-oss-20b` via OpenAI Python client | `SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', prompt)` in SQL |
| Response Parser | Custom `parse_llm_response()` required (list of dicts) | **Not needed** — Cortex returns clean text |
| ai_query() extraction | `get_json_object(get_json_object(ai_query(...), '$[1]'), '$.text')` | `CORTEX.COMPLETE()` returns text directly |
| Embeddings | `all-MiniLM-L6-v2` local model | `CORTEX.EMBED_TEXT_768('e5-base-v2', text)` managed API |
| Vector Store | FAISS in-memory (ephemeral) | Cortex Search Service (managed, persistent) |
| Temperature | 0.3 | 0.3 (same) |

### 9.2 UC1: Data Quality Reporter → dq_audit_report.sql

```sql
{{ config(materialized='table', schema='GOLD') }}

WITH rejection_groups AS (
    SELECT
        REPLACE(source_table, 'clean_', '') AS entity,
        affected_field,
        issue_type,
        COUNT(*) AS rejected_count,
        ARRAY_TO_STRING(ARRAY_SLICE(ARRAY_AGG(record_data), 0, 3), ' | ') AS sample_records
    FROM {{ ref('rejected_records') }}
    GROUP BY source_table, affected_field, issue_type
),

audit AS (
    SELECT entity, bronze_count AS total_entity_records
    FROM {{ ref('pipeline_audit_log') }}
),

enriched AS (
    SELECT
        rg.entity,
        rg.affected_field,
        rg.issue_type,
        rg.rejected_count,
        a.total_entity_records,
        ROUND(rg.rejected_count * 100.0 / NULLIF(a.total_entity_records, 0), 2) AS rejection_rate_pct,
        rg.sample_records
    FROM rejection_groups rg
    LEFT JOIN audit a ON rg.entity = a.entity
)

SELECT
    entity,
    affected_field,
    issue_type,
    rejected_count,
    total_entity_records,
    rejection_rate_pct,
    sample_records,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are a data quality analyst for GlobalMart, a US retail chain. '
        || 'Explain this data quality issue in 3-4 business-friendly sentences. '
        || 'Do NOT use technical jargon like NULL, parse failure, schema, or boolean. '
        || 'Name the specific field and sample values. '
        || 'Identify which business report is at risk. '
        || 'Recommend what the data steward should investigate. '
        || 'Entity: ' || entity
        || '. Field: ' || affected_field
        || '. Issue: ' || issue_type
        || '. Rejected: ' || rejected_count::VARCHAR
        || ' out of ' || total_entity_records::VARCHAR
        || ' records (' || rejection_rate_pct::VARCHAR || '%). '
        || 'Samples: ' || COALESCE(sample_records, 'none'),
        {'temperature': 0.3, 'max_tokens': 512}
    ) AS ai_explanation,
    CURRENT_TIMESTAMP() AS generated_at
FROM enriched
```

### 9.3 UC2: Fraud Investigator → flagged_return_customers.sql

```sql
{{ config(materialized='table', schema='GOLD') }}

WITH profiles AS (
    SELECT * FROM {{ ref('mv_customer_return_history') }}
),

thresholds AS (
    SELECT
        GREATEST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_returns), 3) AS p75_returns,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_refund_value) AS p75_refund,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_refund_per_return) AS p75_avg_refund
    FROM profiles
),

scored AS (
    SELECT
        p.*,
        -- 5 weighted anomaly rules
        CASE WHEN p.total_returns > t.p75_returns THEN 20 ELSE 0 END AS score_high_return_count,
        CASE WHEN p.total_refund_value > t.p75_refund THEN 25 ELSE 0 END AS score_high_total_refund,
        CASE WHEN p.avg_refund_per_return > t.p75_avg_refund THEN 20 ELSE 0 END AS score_high_avg_refund,
        CASE WHEN p.approval_rate_pct > 90 AND p.total_returns >= 2 THEN 15 ELSE 0 END AS score_high_approval_rate,
        CASE WHEN p.distinct_reason_count >= 3 THEN 20 ELSE 0 END AS score_multiple_reasons
    FROM profiles p
    CROSS JOIN thresholds t
),

with_totals AS (
    SELECT
        *,
        score_high_return_count + score_high_total_refund + score_high_avg_refund
            + score_high_approval_rate + score_multiple_reasons AS anomaly_score,
        ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN score_high_return_count > 0 THEN 'high_return_count (20)' END,
            CASE WHEN score_high_total_refund > 0 THEN 'high_total_refund (25)' END,
            CASE WHEN score_high_avg_refund > 0 THEN 'high_avg_refund (20)' END,
            CASE WHEN score_high_approval_rate > 0 THEN 'high_approval_rate (15)' END,
            CASE WHEN score_multiple_reasons > 0 THEN 'multiple_reasons (20)' END
        ), ', ') AS rules_violated
    FROM scored
),

flagged AS (
    SELECT * FROM with_totals WHERE anomaly_score >= 40
)

SELECT
    customer_id,
    customer_name,
    segment,
    region,
    total_returns,
    total_refund_value,
    avg_refund_per_return,
    approval_rate_pct AS approval_rate,
    distinct_reason_count,
    return_reasons,
    anomaly_score,
    rules_violated,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are a fraud investigator for GlobalMart. Write a 4-5 sentence investigation brief. '
        || 'Cite actual numbers. Acknowledge one innocent explanation. '
        || 'Identify the strongest fraud signal. Recommend exactly 2 specific next actions. '
        || 'Customer: ' || customer_name || ' (' || customer_id || '). '
        || 'Segment: ' || segment || '. Region: ' || region || '. '
        || 'Returns: ' || total_returns::VARCHAR || '. '
        || 'Total refunded: $' || total_refund_value::VARCHAR || '. '
        || 'Avg refund: $' || avg_refund_per_return::VARCHAR || '. '
        || 'Approval rate: ' || approval_rate_pct::VARCHAR || '%. '
        || 'Reasons: ' || COALESCE(return_reasons, 'N/A') || '. '
        || 'Score: ' || anomaly_score::VARCHAR || '/100. '
        || 'Rules: ' || rules_violated,
        {'temperature': 0.3, 'max_tokens': 512}
    ) AS investigation_brief,
    CURRENT_TIMESTAMP() AS generated_at
FROM flagged
```

### 9.4 UC3: Product RAG → Cortex Search Service

UC3 requires a **Snowpark Python model** because it builds documents, creates a Cortex Search Service, and runs 5 test queries.

**Step 1: Create RAG documents table and Cortex Search Service (setup SQL)**

```sql
-- Create documents table for search service
CREATE TABLE IF NOT EXISTS GLOBALMART.GOLD.RAG_DOCUMENTS (
    doc_id VARCHAR,
    doc_type VARCHAR,       -- product_summary, product_region, vendor_summary
    doc_text VARCHAR,
    product_id VARCHAR,
    vendor_id VARCHAR,
    region VARCHAR,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create Cortex Search Service on the documents table
CREATE OR REPLACE CORTEX SEARCH SERVICE GLOBALMART.GOLD.PRODUCT_SEARCH_SERVICE
  ON doc_text
  ATTRIBUTES doc_type, product_id, vendor_id, region
  WAREHOUSE = GLOBALMART_CORTEX_WH
  TARGET_LAG = '1 hour'
AS (
    SELECT doc_id, doc_type, doc_text, product_id, vendor_id, region, updated_at
    FROM GLOBALMART.GOLD.RAG_DOCUMENTS
);
```

**Step 2: rag_query_history.py (Snowpark Python dbt model)**

The Python model:
1. Builds document chunks from Gold/Metrics tables
2. Inserts into `RAG_DOCUMENTS`
3. Queries Cortex Search Service for each of 5 test questions
4. Calls `CORTEX.COMPLETE()` with retrieved context
5. Returns query history dataframe

**5 Test Questions:**
1. "Which products are slow-moving and have low sales?"
2. "Which vendor has the highest return rate?"
3. "What are the top-selling products in the East region?"
4. "Which products have the highest return cost?"
5. "Which vendors are active in the West region and what are their sales?"

**Output schema:** question, answer, retrieved_documents, retrieved_count, top_distance, generated_at

### 9.5 UC4: Executive Intelligence → ai_business_insights.sql

```sql
{{ config(materialized='table', schema='GOLD') }}

WITH revenue_kpis AS (
    SELECT
        ROUND(SUM(total_revenue), 2) AS grand_total_revenue,
        ROUND(SUM(total_profit), 2) AS grand_total_profit,
        SUM(total_quantity) AS grand_total_units,
        SUM(order_count) AS grand_total_orders
    FROM {{ ref('mv_monthly_revenue_by_region') }}
),

revenue_by_region AS (
    SELECT
        region,
        ROUND(SUM(total_revenue), 2) AS revenue,
        ROUND(SUM(total_profit), 2) AS profit
    FROM {{ ref('mv_monthly_revenue_by_region') }}
    GROUP BY region
),

revenue_summary AS (
    SELECT
        'revenue_performance' AS insight_type,
        OBJECT_CONSTRUCT(
            'grand_total_revenue', k.grand_total_revenue,
            'grand_total_profit', k.grand_total_profit,
            'grand_total_units', k.grand_total_units,
            'by_region', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('region', region, 'revenue', revenue, 'profit', profit)) FROM revenue_by_region)
        )::VARCHAR AS kpi_data
    FROM revenue_kpis k
),

vendor_kpis AS (
    SELECT
        'vendor_return_rate' AS insight_type,
        OBJECT_CONSTRUCT(
            'vendors', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'vendor', vendor_name,
                'total_sales', total_sales,
                'return_rate', return_rate_pct,
                'total_refunded', total_refunded
            )) FROM {{ ref('mv_return_rate_by_vendor') }})
        )::VARCHAR AS kpi_data
),

inventory_kpis AS (
    SELECT
        'slow_moving_inventory' AS insight_type,
        OBJECT_CONSTRUCT(
            'total_return_cost', (SELECT ROUND(SUM(total_return_cost), 2) FROM {{ ref('mv_product_return_impact') }}),
            'avg_return_cost', (SELECT ROUND(AVG(avg_return_cost), 2) FROM {{ ref('mv_product_return_impact') }}),
            'top_5', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('product', product_name, 'region', region, 'cost', total_return_cost))
                      FROM (SELECT * FROM {{ ref('mv_product_return_impact') }} ORDER BY total_return_cost DESC LIMIT 5))
        )::VARCHAR AS kpi_data
),

all_domains AS (
    SELECT * FROM revenue_summary
    UNION ALL SELECT * FROM vendor_kpis
    UNION ALL SELECT * FROM inventory_kpis
)

SELECT
    insight_type,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are an executive analyst for GlobalMart. Write a 4-6 sentence executive summary. '
        || 'Cite specific dollar amounts and percentages. Identify strongest and weakest areas. '
        || 'Include one actionable recommendation. '
        || 'Domain: ' || insight_type || '. KPIs: ' || kpi_data,
        {'temperature': 0.3, 'max_tokens': 512}
    ) AS executive_summary,
    kpi_data,
    CURRENT_TIMESTAMP() AS generated_at
FROM all_domains
```

**Cortex SQL-native LLM demos (equivalent to Databricks `ai_query()`):**

```sql
-- Demo 1: Revenue Assessment per Region
SELECT
    region,
    total_revenue,
    total_profit,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'In one sentence, assess the revenue health of the ' || region
        || ' region with total revenue of $' || total_revenue::VARCHAR
        || ' and profit of $' || total_profit::VARCHAR
        || '. Is this region performing well or underperforming?',
        {'temperature': 0.3}
    ) AS ai_assessment
FROM (
    SELECT region, SUM(total_revenue) AS total_revenue, SUM(total_profit) AS total_profit
    FROM GLOBALMART.METRICS.MV_MONTHLY_REVENUE_BY_REGION
    GROUP BY region
);

-- Demo 2: Vendor Risk Assessment
SELECT
    vendor_name,
    total_sales,
    return_rate_pct,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'In one sentence, should GlobalMart continue, renegotiate, or drop vendor "'
        || vendor_name || '" who has $' || total_sales::VARCHAR
        || ' in sales and a ' || return_rate_pct::VARCHAR
        || '% return rate? Give a clear recommendation.',
        {'temperature': 0.3}
    ) AS ai_vendor_recommendation
FROM GLOBALMART.GOLD.MV_RETURN_RATE_BY_VENDOR;
```

---

## 10. Orchestration

### 10.1 Snowflake Tasks DAG

```sql
-- Root task: triggered by streams (see Section 2.10)
-- Child tasks: run dbt layers in sequence

CREATE OR REPLACE TASK GLOBALMART.RAW.DBT_BRONZE
  WAREHOUSE = GLOBALMART_TRANSFORM_WH
  AFTER GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA
AS
  CALL SYSTEM$EXECUTE_DBT('dbt run --select bronze');

CREATE OR REPLACE TASK GLOBALMART.RAW.DBT_SILVER
  WAREHOUSE = GLOBALMART_TRANSFORM_WH
  AFTER GLOBALMART.RAW.DBT_BRONZE
AS
  CALL SYSTEM$EXECUTE_DBT('dbt run --select silver');

CREATE OR REPLACE TASK GLOBALMART.RAW.DBT_GOLD
  WAREHOUSE = GLOBALMART_TRANSFORM_WH
  AFTER GLOBALMART.RAW.DBT_SILVER
AS
  CALL SYSTEM$EXECUTE_DBT('dbt run --select gold');

CREATE OR REPLACE TASK GLOBALMART.RAW.DBT_GENAI
  WAREHOUSE = GLOBALMART_CORTEX_WH
  AFTER GLOBALMART.RAW.DBT_GOLD
AS
  CALL SYSTEM$EXECUTE_DBT('dbt run --select genai');
```

### 10.2 Alternative: dbt Cloud Job Scheduling

If using dbt Cloud instead of Snowflake Tasks:
1. Create a dbt Cloud project linked to the git repo
2. Create a job with: `dbt run --full-refresh` (first run) then `dbt run` (incremental)
3. Schedule: every 5 minutes or trigger via API webhook from Snowpipe notification

---

## 11. Cortex Analyst — Semantic Model

Cortex Analyst replaces Databricks Genie Space. It requires a YAML semantic model file uploaded to a Snowflake stage.

### 11.1 semantic_model.yaml

```yaml
name: GlobalMart Data Intelligence
description: >
  Semantic model for GlobalMart, a 5-region US retail chain.
  Solves 3 business failures: Revenue Audit (9% overstatement),
  Returns Fraud ($2.3M annual loss), Inventory Blindspot (12-18% revenue lost).

tables:
  - name: dim_customers
    database: GLOBALMART
    schema: GOLD
    description: "374 unique customers (deduped from 748 raw). Segments: Consumer, Corporate, Home Office."
    columns:
      - name: customer_sk
        description: Surrogate key for joins
      - name: customer_id
        description: Business key
      - name: customer_name
        description: Full name
      - name: segment
        description: "Consumer, Corporate, or Home Office"
      - name: region
        description: "East, West, South, Central, or North"

  - name: dim_products
    database: GLOBALMART
    schema: GOLD
    description: Product catalog with brand and category
    columns:
      - name: product_sk
        description: Surrogate key
      - name: product_id
        description: Business key
      - name: product_name
        description: Display name

  - name: dim_vendors
    database: GLOBALMART
    schema: GOLD
    description: Vendor reference
    columns:
      - name: vendor_sk
        description: Surrogate key
      - name: vendor_name
        description: Vendor company name

  - name: dim_dates
    database: GLOBALMART
    schema: GOLD
    description: "Calendar 2016-2018"
    columns:
      - name: date_sk
        description: Surrogate key
      - name: date_key
        description: Calendar date

  - name: fact_sales
    database: GLOBALMART
    schema: GOLD
    description: "One row per order line item. Measures: sales, quantity, discount, profit."
    columns:
      - name: customer_sk
        description: FK to dim_customers
      - name: product_sk
        description: FK to dim_products
      - name: vendor_sk
        description: FK to dim_vendors
      - name: order_date_sk
        description: FK to dim_dates
      - name: sales
        description: Revenue amount
      - name: quantity
        description: Units sold
      - name: profit
        description: Profit amount

  - name: fact_returns
    database: GLOBALMART
    schema: GOLD
    description: "One row per return event. Measures: refund_amount."
    columns:
      - name: customer_sk
        description: FK to dim_customers
      - name: return_date_sk
        description: FK to dim_dates
      - name: refund_amount
        description: Refund dollars
      - name: return_status
        description: "Approved, Pending, or Rejected"

joins:
  - left_table: fact_sales
    right_table: dim_customers
    condition: "customer_sk = customer_sk"
    type: many_to_one
  - left_table: fact_sales
    right_table: dim_products
    condition: "product_sk = product_sk"
    type: many_to_one
  - left_table: fact_sales
    right_table: dim_vendors
    condition: "vendor_sk = vendor_sk"
    type: many_to_one
  - left_table: fact_sales
    right_table: dim_dates
    condition: "order_date_sk = date_sk"
    type: many_to_one
  - left_table: fact_returns
    right_table: dim_customers
    condition: "customer_sk = customer_sk"
    type: many_to_one
  - left_table: fact_returns
    right_table: dim_dates
    condition: "return_date_sk = date_sk"
    type: many_to_one

sample_questions:
  - "What is the total revenue by region?"
  - "Which vendors have the highest return rate?"
  - "Show me the top 5 products by revenue in the West region"
  - "Which customers have the highest return refunds?"
  - "What is the profit margin by segment?"
```

---

## 12. Query Walkthroughs (Snowflake SQL)

### Q1: Total Revenue by Region in 2018

```sql
SELECT c.region, ROUND(SUM(f.sales), 2) AS total_revenue
FROM GLOBALMART.GOLD.FACT_SALES f
JOIN GLOBALMART.GOLD.DIM_CUSTOMERS c ON f.customer_sk = c.customer_sk
JOIN GLOBALMART.GOLD.DIM_DATES d ON f.order_date_sk = d.date_sk
WHERE d.year = 2018
GROUP BY c.region
ORDER BY total_revenue DESC;
```

### Q2: Top 10 Customers by Return Refunds

```sql
SELECT c.customer_name, c.region, ROUND(SUM(r.refund_amount), 2) AS total_refunded
FROM GLOBALMART.GOLD.FACT_RETURNS r
JOIN GLOBALMART.GOLD.DIM_CUSTOMERS c ON r.customer_sk = c.customer_sk
GROUP BY c.customer_name, c.region
ORDER BY total_refunded DESC
LIMIT 10;
```

### Q3: Return Rate by Vendor (Pre-Computed)

```sql
SELECT vendor_name, total_orders, return_order_count, return_rate_pct
FROM GLOBALMART.GOLD.MV_RETURN_RATE_BY_VENDOR
ORDER BY return_rate_pct DESC;
```

### Q4: Products with Highest Allocated Return Costs

```sql
SELECT p.product_name, ROUND(SUM(b.allocated_refund), 2) AS total_allocated_refund
FROM GLOBALMART.GOLD.BRIDGE_RETURN_PRODUCTS b
JOIN GLOBALMART.GOLD.DIM_PRODUCTS p ON b.product_sk = p.product_sk
GROUP BY p.product_name
ORDER BY total_allocated_refund DESC
LIMIT 10;
```

### Q5: Slow-Moving Products by Region

```sql
SELECT product_name, region, total_sales, total_quantity, is_slow_moving
FROM GLOBALMART.GOLD.MV_SLOW_MOVING_PRODUCTS
WHERE is_slow_moving = TRUE
ORDER BY total_quantity ASC;
```

---

## 13. Production Considerations

### 13.1 CDC Pipeline (vs Full Reprocess)
The Databricks version re-processes all data each run. Snowflake's architecture with Streams + incremental dbt models implements true CDC — only new/changed rows flow through the pipeline.

### 13.2 Vector Store Persistence
Databricks UC3 builds an ephemeral FAISS index in memory on every notebook run. Snowflake Cortex Search Service is persistent and managed — the index survives across sessions and auto-updates when the source table changes.

### 13.3 Response Parser Elimination
`databricks-gpt-oss-20b` returns `[{"type":"reasoning",...}, {"type":"text","text":"answer"}]` requiring a custom Python parser in all 4 GenAI notebooks. Snowflake Cortex COMPLETE returns clean text — no parser needed, major simplification.

### 13.4 Monitoring Dashboard
Both `rejected_records` and `pipeline_audit_log` contain everything needed for a data quality dashboard. In Snowflake, use Snowsight dashboards with alerts when rejection rates exceed thresholds.

### 13.5 Data Contracts
Silver ephemeral models enforce schema contracts with explicit CASE/COALESCE transformations and `_fail_*` boolean flags. dbt tests in `_silver__models.yml` validate all 23 quality rules on every run.

---

## 14. Deployment Script (deploy.sh)

```bash
#!/bin/bash
set -e

echo "=== GlobalMart Snowflake + dbt Deployment ==="

# Step 1: Run setup SQL scripts
echo "[1/7] Creating infrastructure..."
snowsql -f setup/01_create_infrastructure.sql

echo "[2/7] Creating stage and uploading files..."
snowsql -f setup/02_create_stage_and_upload.sql

echo "[3/7] Creating landing tables..."
snowsql -f setup/03_create_landing_tables.sql

echo "[4/7] Running initial data seed..."
snowsql -f setup/04_initial_seed.sql

echo "[5/7] Creating Snowpipes..."
snowsql -f setup/05_create_snowpipes.sql

echo "[6/7] Creating streams and tasks..."
snowsql -f setup/06_create_streams_and_tasks.sql

# Step 2: Run dbt
echo "[7/7] Running dbt..."
cd globalmart_dbt
dbt deps
dbt run --full-refresh
dbt test

# Step 3: GenAI setup + run
echo "[8/8] Setting up GenAI..."
cd ..
snowsql -f setup/07_genai_cortex.sql
cd globalmart_dbt
dbt run --select genai

echo "=== Deployment Complete ==="
echo "Next: Upload cortex_analyst/semantic_model.yaml to set up Cortex Analyst"
```

---

## 15. Table Count Verification

| Layer | Count | Tables |
|---|---|---|
| RAW (landing) | 6 | RAW_*_LOAD (VARIANT) |
| Bronze (dbt) | 6 | raw_customers, raw_orders, raw_transactions, raw_returns, raw_products, raw_vendors |
| Silver (dbt) | 8 | 6 clean_* + rejected_records + pipeline_audit_log |
| Gold (dbt) | 10 | 4 dims + 2 facts + 1 bridge + 3 MVs |
| Metrics (dbt) | 6 | 6 mv_* views |
| GenAI (dbt) | 4 | dq_audit_report, flagged_return_customers, rag_query_history, ai_business_insights |
| **Total** | **40** | 6 RAW + 34 dbt models |
