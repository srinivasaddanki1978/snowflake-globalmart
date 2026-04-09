-- ============================================================
-- GlobalMart: Step 4 — Initial Seed (one-time COPY INTO)
-- CSV: OBJECT_CONSTRUCT_KEEP_NULL(*) packs columns into VARIANT
-- JSON: $1 captures full JSON object as VARIANT
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GLOBALMART;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

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

-- Verify counts
SELECT 'RAW_CUSTOMERS_LOAD' AS table_name, COUNT(*) AS row_count FROM GLOBALMART.RAW.RAW_CUSTOMERS_LOAD
UNION ALL SELECT 'RAW_ORDERS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_ORDERS_LOAD
UNION ALL SELECT 'RAW_TRANSACTIONS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD
UNION ALL SELECT 'RAW_RETURNS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_RETURNS_LOAD
UNION ALL SELECT 'RAW_PRODUCTS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_PRODUCTS_LOAD
UNION ALL SELECT 'RAW_VENDORS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_VENDORS_LOAD;
