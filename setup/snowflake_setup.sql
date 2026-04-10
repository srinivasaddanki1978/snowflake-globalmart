-- ============================================================
-- GlobalMart: Snowflake Setup (All Objects)
--
-- Creates everything needed before dbt: database, schemas,
-- file formats, internal stage, RAW landing tables, SEED
-- stored procedure, streams, and task DAG.
--
-- Idempotent — safe to run multiple times.
-- Does NOT upload files or seed data (deploy.py handles that).
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. Database + Schemas
-- ============================================================

CREATE DATABASE IF NOT EXISTS GLOBALMART;

CREATE SCHEMA IF NOT EXISTS GLOBALMART.RAW;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.BRONZE;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.SILVER;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.GOLD;
CREATE SCHEMA IF NOT EXISTS GLOBALMART.METRICS;

-- ============================================================
-- 2. File Formats
-- ============================================================

-- Standard CSV format (used by stage queries)
CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON format (STRIP_OUTER_ARRAY for array-of-objects files)
CREATE OR REPLACE FILE FORMAT GLOBALMART.RAW.JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- CSV format with PARSE_HEADER for INFER_SCHEMA + MATCH_BY_COLUMN_NAME
-- Used by the SEED_RAW_TABLES procedure to read CSV column names
CREATE FILE FORMAT IF NOT EXISTS GLOBALMART.RAW.CSV_PARSE_HEADER_FORMAT
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================================
-- 3. Internal Stage
-- ============================================================

CREATE STAGE IF NOT EXISTS GLOBALMART.RAW.SOURCE_DATA_STAGE
  COMMENT = 'Landing zone for GlobalMart regional source files';

-- ============================================================
-- 4. RAW Landing Tables (6 VARIANT tables)
-- ============================================================

CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_CUSTOMERS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_ORDERS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_RETURNS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_PRODUCTS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GLOBALMART.RAW.RAW_VENDORS_LOAD (
    raw_data        VARIANT,
    _source_file    VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 5. SEED_RAW_TABLES Stored Procedure
--
-- Two modes:
--   CALL SEED_RAW_TABLES('full')        — truncate + reload all
--   CALL SEED_RAW_TABLES('incremental') — only new files
--
-- CSV: Uses INFER_SCHEMA to read column names from header,
--   creates a named-column temp table, COPY INTO with
--   MATCH_BY_COLUMN_NAME, then OBJECT_CONSTRUCT_KEEP_NULL(*)
--   produces proper JSON keys.
--
-- JSON: Direct COPY INTO with $1 (works natively).
-- ============================================================

CREATE OR REPLACE PROCEDURE GLOBALMART.RAW.SEED_RAW_TABLES(MODE VARCHAR DEFAULT 'full')
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var STAGE = '@GLOBALMART.RAW.SOURCE_DATA_STAGE';
    var mode = MODE.toLowerCase();

    if (mode !== 'full' && mode !== 'incremental') {
        return "ERROR: MODE must be 'full' or 'incremental'. Got: " + MODE;
    }

    var entities = [
        {table: 'RAW_CUSTOMERS_LOAD',    pattern: '.*customers.*[.]csv',    type: 'csv'},
        {table: 'RAW_ORDERS_LOAD',       pattern: '.*orders.*[.]csv',       type: 'csv'},
        {table: 'RAW_TRANSACTIONS_LOAD', pattern: '.*transactions.*[.]csv', type: 'csv'},
        {table: 'RAW_RETURNS_LOAD',      pattern: '.*returns.*[.]json',     type: 'json'},
        {table: 'RAW_PRODUCTS_LOAD',     pattern: '.*products[.]json',      type: 'json'},
        {table: 'RAW_VENDORS_LOAD',      pattern: '.*vendors[.]csv',        type: 'csv'}
    ];

    var log = [];
    var totalRows = 0;
    log.push('Mode: ' + mode);
    log.push('');

    if (mode === 'full') {
        for (var i = 0; i < entities.length; i++) {
            snowflake.execute({sqlText:
                'TRUNCATE TABLE IF EXISTS GLOBALMART.RAW.' + entities[i].table
            });
        }
        log.push('All RAW tables truncated.');
        log.push('');
    }

    for (var i = 0; i < entities.length; i++) {
        var entity = entities[i];
        var entityRows = 0;

        if (entity.type === 'json') {
            var forceClause = (mode === 'full') ? ' FORCE = TRUE' : '';
            var copySql =
                "COPY INTO GLOBALMART.RAW." + entity.table +
                " (raw_data, _source_file, _load_timestamp) FROM (" +
                " SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()" +
                " FROM " + STAGE +
                " (FILE_FORMAT => 'GLOBALMART.RAW.JSON_FORMAT'," +
                "  PATTERN => '" + entity.pattern + "')" +
                ")" + forceClause;

            try {
                var copyResult = snowflake.execute({sqlText: copySql});
                while (copyResult.next()) {
                    try {
                        var fileName = copyResult.getColumnValue(1);
                        var status = copyResult.getColumnValue(2);
                        var rowsLoaded = copyResult.getColumnValue(4);
                        if (status === 'LOAD_SKIPPED') {
                            continue;
                        }
                        entityRows += rowsLoaded;
                        log.push('  ' + fileName + ': ' + rowsLoaded + ' rows');
                    } catch (colErr) {
                        break;
                    }
                }
            } catch (err) {
                log.push('  ERROR (JSON): ' + err.message);
            }

        } else {
            var listStmt = snowflake.execute({
                sqlText: "LIST " + STAGE + " PATTERN = '" + entity.pattern + "'"
            });

            var files = [];
            while (listStmt.next()) {
                var fullPath = listStmt.getColumnValue(1);
                var slashIdx = fullPath.indexOf('/');
                var relPath = (slashIdx >= 0)
                    ? fullPath.substring(slashIdx + 1)
                    : fullPath;
                files.push(relPath);
            }

            if (files.length === 0) {
                log.push('  WARNING: No files found for ' + entity.table);
                log.push('');
                continue;
            }

            for (var j = 0; j < files.length; j++) {
                var relPath = files[j];
                var stageFile = STAGE + '/' + relPath;
                var safeRelPath = relPath.replace(/'/g, "''");

                if (mode === 'incremental') {
                    var checkStmt = snowflake.execute({sqlText:
                        "SELECT COUNT(*) FROM GLOBALMART.RAW." + entity.table +
                        " WHERE _source_file = '" + safeRelPath + "'"
                    });
                    checkStmt.next();
                    if (checkStmt.getColumnValue(1) > 0) {
                        continue;
                    }
                }

                try {
                    var inferStmt = snowflake.execute({sqlText:
                        "SELECT COLUMN_NAME FROM TABLE(INFER_SCHEMA(" +
                        "  LOCATION => '" + stageFile + "'," +
                        "  FILE_FORMAT => 'GLOBALMART.RAW.CSV_PARSE_HEADER_FORMAT'" +
                        ")) ORDER BY ORDER_ID"
                    });

                    var columns = [];
                    while (inferStmt.next()) {
                        columns.push(inferStmt.getColumnValue(1));
                    }

                    if (columns.length === 0) {
                        log.push('  WARNING: No columns inferred for ' + relPath);
                        continue;
                    }

                    var colDefs = columns.map(function(c) {
                        return '"' + c.replace(/"/g, '""') + '" VARCHAR';
                    }).join(', ');

                    snowflake.execute({sqlText:
                        "CREATE OR REPLACE TEMP TABLE GLOBALMART.RAW._TEMP_CSV_LOAD (" +
                        colDefs + ")"
                    });

                    snowflake.execute({sqlText:
                        "COPY INTO GLOBALMART.RAW._TEMP_CSV_LOAD " +
                        "FROM " + stageFile + " " +
                        "FILE_FORMAT = (TYPE='CSV' PARSE_HEADER=TRUE " +
                        "FIELD_OPTIONALLY_ENCLOSED_BY='\"' " +
                        "NULL_IF=('','NULL','null') TRIM_SPACE=TRUE " +
                        "ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE) " +
                        "MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'"
                    });

                    var countStmt = snowflake.execute({sqlText:
                        "SELECT COUNT(*) FROM GLOBALMART.RAW._TEMP_CSV_LOAD"
                    });
                    countStmt.next();
                    var rowCount = countStmt.getColumnValue(1);

                    snowflake.execute({sqlText:
                        "INSERT INTO GLOBALMART.RAW." + entity.table +
                        " (raw_data, _source_file, _load_timestamp)" +
                        " SELECT OBJECT_CONSTRUCT_KEEP_NULL(*)," +
                        " '" + safeRelPath + "'," +
                        " CURRENT_TIMESTAMP()" +
                        " FROM GLOBALMART.RAW._TEMP_CSV_LOAD"
                    });

                    entityRows += rowCount;
                    log.push('  ' + relPath + ': ' + rowCount + ' rows (new)');

                } catch (err) {
                    log.push('  ERROR (' + relPath + '): ' + err.message);
                } finally {
                    try {
                        snowflake.execute({sqlText:
                            "DROP TABLE IF EXISTS GLOBALMART.RAW._TEMP_CSV_LOAD"
                        });
                    } catch (e) { /* ignore */ }
                }
            }
        }

        totalRows += entityRows;
        if (entityRows > 0) {
            log.push(entity.table + ' total: ' + entityRows + ' rows');
            log.push('');
        } else if (mode === 'incremental') {
            log.push(entity.table + ': no new files');
        } else {
            log.push(entity.table + ' total: 0 rows');
            log.push('');
        }
    }

    log.push('');
    log.push('========================================');
    log.push('Total: ' + totalRows + ' rows loaded');
    log.push('========================================');

    return log.join('\n');
$$;

-- ============================================================
-- 6. Streams (APPEND_ONLY on each RAW table)
-- ============================================================

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

-- ============================================================
-- 7. dbt Pipeline Procedure (placeholder)
--    In production: calls dbt Cloud API or runs dbt CLI
--    via external function / Snowpark container
-- ============================================================

CREATE OR REPLACE PROCEDURE GLOBALMART.RAW.RUN_DBT_PIPELINE()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
BEGIN
  RETURN 'dbt pipeline triggered at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- ============================================================
-- 8. Task DAG (auto-ingest + auto-transform)
--
-- INGEST_NEW_FILES (parent, 5-min schedule)
--   → checks stage for new files, loads into RAW tables
--
-- TRANSFORM_ON_NEW_DATA (child, after parent)
--   → fires only when streams have data → triggers dbt
-- ============================================================

CREATE OR REPLACE TASK GLOBALMART.RAW.INGEST_NEW_FILES
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
AS
  CALL GLOBALMART.RAW.SEED_RAW_TABLES('incremental');

CREATE OR REPLACE TASK GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA
  WAREHOUSE = COMPUTE_WH
  AFTER GLOBALMART.RAW.INGEST_NEW_FILES
  WHEN
    SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.CUSTOMERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.ORDERS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.TRANSACTIONS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.RETURNS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.PRODUCTS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('GLOBALMART.RAW.VENDORS_STREAM')
AS
  CALL GLOBALMART.RAW.RUN_DBT_PIPELINE();

-- ============================================================
-- 9. Git Integration (for Snowflake dbt from Git)
--
-- Allows Snowflake to pull the dbt project directly from GitHub.
-- For public repos: GIT_CREDENTIALS is optional.
-- For private repos: Create a secret with your GitHub PAT first.
-- ============================================================

-- Secret for GitHub credentials (skip for public repos)
-- To use: Replace <YOUR_GITHUB_PAT> with your personal access token
CREATE SECRET IF NOT EXISTS GLOBALMART.RAW.GIT_SECRET
  TYPE = PASSWORD
  USERNAME = 'srinivasaddanki1978'
  PASSWORD = '';

-- API integration for Git HTTPS access
CREATE OR REPLACE API INTEGRATION GLOBALMART_GIT_INTEGRATION
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ('https://github.com/srinivasaddanki1978/')
  ALLOWED_AUTHENTICATION_SECRETS = (GLOBALMART.RAW.GIT_SECRET)
  ENABLED = TRUE;

-- Network rule for dbt package downloads (hub.getdbt.com, github.com)
CREATE OR REPLACE NETWORK RULE GLOBALMART.RAW.GLOBALMART_DBT_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'github.com', 'codeload.github.com', 'raw.githubusercontent.com', 'objects.githubusercontent.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GLOBALMART_DBT_EXT_ACCESS
  ALLOWED_NETWORK_RULES = (GLOBALMART.RAW.GLOBALMART_DBT_NETWORK_RULE)
  ENABLED = TRUE;

-- Git repository connection
CREATE OR REPLACE GIT REPOSITORY GLOBALMART.RAW.SNOWFLAKE_GLOBALMART
  API_INTEGRATION = GLOBALMART_GIT_INTEGRATION
  GIT_CREDENTIALS = GLOBALMART.RAW.GIT_SECRET
  ORIGIN = 'https://github.com/srinivasaddanki1978/snowflake-globalmart.git';
