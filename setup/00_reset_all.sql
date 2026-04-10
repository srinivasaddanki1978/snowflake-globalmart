-- ============================================================
-- GlobalMart: Step 0 — FULL RESET (Drop Everything, Keep Stage)
-- Run this BEFORE scripts 01-07 to get a clean start.
--
-- PRESERVES: Internal stage (SOURCE_DATA_STAGE) with uploaded files
-- DROPS: All tables, views, pipes, streams, tasks across all schemas
-- RECREATES: Database, schemas, file formats, RAW tables
-- Then reloads data from stage into RAW tables
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- PHASE 1: Tear down objects that depend on RAW tables
-- (Must happen BEFORE dropping tables/schemas)
-- ============================================================

-- Suspend the task first (prevents errors during drop)
ALTER TASK IF EXISTS GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA SUSPEND;

-- Drop task and stored procedure
DROP TASK IF EXISTS GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA;
DROP PROCEDURE IF EXISTS GLOBALMART.RAW.RUN_DBT_PIPELINE();

-- Drop all 6 streams
DROP STREAM IF EXISTS GLOBALMART.RAW.CUSTOMERS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.ORDERS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.TRANSACTIONS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.RETURNS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.PRODUCTS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.VENDORS_STREAM;

-- Drop all 6 pipes
DROP PIPE IF EXISTS GLOBALMART.RAW.CUSTOMERS_PIPE;
DROP PIPE IF EXISTS GLOBALMART.RAW.ORDERS_PIPE;
DROP PIPE IF EXISTS GLOBALMART.RAW.TRANSACTIONS_PIPE;
DROP PIPE IF EXISTS GLOBALMART.RAW.RETURNS_PIPE;
DROP PIPE IF EXISTS GLOBALMART.RAW.PRODUCTS_PIPE;
DROP PIPE IF EXISTS GLOBALMART.RAW.VENDORS_PIPE;

-- Drop GenAI objects
DROP CORTEX SEARCH SERVICE IF EXISTS GLOBALMART.GOLD.PRODUCT_SEARCH_SERVICE;

-- ============================================================
-- PHASE 2: Drop all dbt-created schemas (CASCADE removes
-- all tables, views, etc. inside them)
-- ============================================================

DROP SCHEMA IF EXISTS GLOBALMART.BRONZE CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.SILVER CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.GOLD CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.METRICS CASCADE;

-- Also clean up any leftover PUBLIC_* schemas from before
-- the generate_schema_name macro fix
DROP SCHEMA IF EXISTS GLOBALMART.PUBLIC_BRONZE CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.PUBLIC_SILVER CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.PUBLIC_GOLD CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.PUBLIC_METRICS CASCADE;

-- ============================================================
-- PHASE 3: Drop and recreate RAW tables
-- (Dropping resets COPY INTO load history so files reload cleanly)
-- NOTE: We do NOT drop RAW schema — it holds the stage + formats
-- ============================================================

DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_CUSTOMERS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_ORDERS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_RETURNS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_PRODUCTS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_VENDORS_LOAD;

-- Also drop any variant-named tables from earlier iterations
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_CUSTOMERS_LOAD1;

-- ============================================================
-- PHASE 4: Recreate schemas (required by dbt — must pre-exist)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS GLOBALMART.BRONZE;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.SILVER;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.GOLD;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.METRICS;

-- ============================================================
-- PHASE 5: Recreate file formats (in case they were modified)
-- ============================================================

CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- ============================================================
-- PHASE 6: Recreate the 6 RAW landing tables
-- ============================================================

CREATE TABLE GLOBALMART.RAW.RAW_CUSTOMERS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE GLOBALMART.RAW.RAW_ORDERS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE GLOBALMART.RAW.RAW_RETURNS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE GLOBALMART.RAW.RAW_PRODUCTS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE GLOBALMART.RAW.RAW_VENDORS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- PHASE 7: Reload data from stage (files are already there)
-- ============================================================

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

-- Transactions: 3 CSV files (Region2, Region4, root)
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

-- ============================================================
-- PHASE 8: Verify row counts
-- ============================================================

SELECT 'RAW_CUSTOMERS_LOAD' AS table_name, COUNT(*) AS row_count FROM GLOBALMART.RAW.RAW_CUSTOMERS_LOAD
UNION ALL SELECT 'RAW_ORDERS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_ORDERS_LOAD
UNION ALL SELECT 'RAW_TRANSACTIONS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD
UNION ALL SELECT 'RAW_RETURNS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_RETURNS_LOAD
UNION ALL SELECT 'RAW_PRODUCTS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_PRODUCTS_LOAD
UNION ALL SELECT 'RAW_VENDORS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_VENDORS_LOAD;
