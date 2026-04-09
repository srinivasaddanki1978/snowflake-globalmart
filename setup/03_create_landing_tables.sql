-- ============================================================
-- GlobalMart: Step 3 — 6 RAW Landing Tables (VARIANT)
-- Each table: raw_data VARIANT + _source_file + _load_timestamp
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GLOBALMART;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

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
