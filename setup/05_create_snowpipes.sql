-- ============================================================
-- GlobalMart: Step 5 — 6 Snowpipes (continuous ingestion)
-- AUTO_INGEST=TRUE — primary ingestion for ongoing data
-- Will NOT re-load files already loaded by COPY INTO (Step 4)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GLOBALMART;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

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

-- Verify pipes
SHOW PIPES IN SCHEMA GLOBALMART.RAW;
