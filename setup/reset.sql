-- ============================================================
-- GlobalMart: Full Reset
--
-- Drops all pipeline objects and recreates from scratch.
-- PRESERVES: Internal stage (SOURCE_DATA_STAGE) with uploaded files.
-- After this, run: CALL GLOBALMART.RAW.SEED_RAW_TABLES('full');
--
-- Or just re-run: python setup/deploy.py
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- PHASE 1: Suspend and drop tasks (child before parent)
-- ============================================================

ALTER TASK IF EXISTS GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA SUSPEND;
ALTER TASK IF EXISTS GLOBALMART.RAW.INGEST_NEW_FILES SUSPEND;

DROP TASK IF EXISTS GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA;
DROP TASK IF EXISTS GLOBALMART.RAW.INGEST_NEW_FILES;

DROP PROCEDURE IF EXISTS GLOBALMART.RAW.RUN_DBT_PIPELINE();
DROP PROCEDURE IF EXISTS GLOBALMART.RAW.SEED_RAW_TABLES(VARCHAR);

-- ============================================================
-- PHASE 2: Drop streams
-- ============================================================

DROP STREAM IF EXISTS GLOBALMART.RAW.CUSTOMERS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.ORDERS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.TRANSACTIONS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.RETURNS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.PRODUCTS_STREAM;
DROP STREAM IF EXISTS GLOBALMART.RAW.VENDORS_STREAM;

-- ============================================================
-- PHASE 3: Drop GenAI objects
-- ============================================================

DROP CORTEX SEARCH SERVICE IF EXISTS GLOBALMART.GOLD.PRODUCT_SEARCH_SERVICE;

-- ============================================================
-- PHASE 4: Drop dbt schemas (CASCADE removes all tables/views)
-- ============================================================

DROP SCHEMA IF EXISTS GLOBALMART.BRONZE CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.SILVER CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.GOLD CASCADE;
DROP SCHEMA IF EXISTS GLOBALMART.METRICS CASCADE;

-- ============================================================
-- PHASE 5: Drop and recreate RAW tables
-- (Dropping resets COPY INTO load history so files reload cleanly)
-- NOTE: We do NOT drop RAW schema — it holds the stage + formats
-- ============================================================

DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_CUSTOMERS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_ORDERS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_RETURNS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_PRODUCTS_LOAD;
DROP TABLE IF EXISTS GLOBALMART.RAW.RAW_VENDORS_LOAD;

-- ============================================================
-- PHASE 6: Recreate everything via snowflake_setup.sql objects
-- ============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS GLOBALMART.BRONZE;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.SILVER;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.GOLD;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.METRICS;

-- File formats
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

CREATE FILE FORMAT IF NOT EXISTS GLOBALMART.RAW.CSV_PARSE_HEADER_FORMAT
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- RAW tables
CREATE TABLE GLOBALMART.RAW.RAW_CUSTOMERS_LOAD (
    raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE GLOBALMART.RAW.RAW_ORDERS_LOAD (
    raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD (
    raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE GLOBALMART.RAW.RAW_RETURNS_LOAD (
    raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE GLOBALMART.RAW.RAW_PRODUCTS_LOAD (
    raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE TABLE GLOBALMART.RAW.RAW_VENDORS_LOAD (
    raw_data VARIANT, _source_file VARCHAR, _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- PHASE 7: Reload data from stage
-- (Stage files are preserved — just re-run the procedure)
-- ============================================================
-- NOTE: The SEED_RAW_TABLES procedure must be recreated first.
-- Either run snowflake_setup.sql or deploy.py to recreate it,
-- then: CALL GLOBALMART.RAW.SEED_RAW_TABLES('full');

-- ============================================================
-- PHASE 8: Verify
-- ============================================================

SELECT 'RAW_CUSTOMERS_LOAD' AS table_name, COUNT(*) AS row_count FROM GLOBALMART.RAW.RAW_CUSTOMERS_LOAD
UNION ALL SELECT 'RAW_ORDERS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_ORDERS_LOAD
UNION ALL SELECT 'RAW_TRANSACTIONS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD
UNION ALL SELECT 'RAW_RETURNS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_RETURNS_LOAD
UNION ALL SELECT 'RAW_PRODUCTS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_PRODUCTS_LOAD
UNION ALL SELECT 'RAW_VENDORS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_VENDORS_LOAD;
