-- ============================================================
-- GlobalMart: Step 2 — Internal Stage + PUT 16 Files
-- Run from SnowSQL with access to data/ folder
-- Paths quoted to handle folder names with spaces
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GLOBALMART;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE STAGE GLOBALMART.RAW.SOURCE_DATA_STAGE
  COMMENT = 'Landing zone for GlobalMart regional source files';

-- Region 1 (2 files)
PUT 'file://data/Region 1/customers_1.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region1/ AUTO_COMPRESS=FALSE;
PUT 'file://data/Region 1/orders_1.csv'    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region1/ AUTO_COMPRESS=FALSE;

-- Region 2 (2 files)
PUT 'file://data/Region 2/customers_2.csv'    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region2/ AUTO_COMPRESS=FALSE;
PUT 'file://data/Region 2/transactions_1.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region2/ AUTO_COMPRESS=FALSE;

-- Region 3 (2 files)
PUT 'file://data/Region 3/customers_3.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region3/ AUTO_COMPRESS=FALSE;
PUT 'file://data/Region 3/orders_2.csv'    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region3/ AUTO_COMPRESS=FALSE;

-- Region 4 (2 files)
PUT 'file://data/Region 4/customers_4.csv'    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region4/ AUTO_COMPRESS=FALSE;
PUT 'file://data/Region 4/transactions_2.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region4/ AUTO_COMPRESS=FALSE;

-- Region 5 (2 files)
PUT 'file://data/Region 5/customers_5.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region5/ AUTO_COMPRESS=FALSE;
PUT 'file://data/Region 5/orders_3.csv'    @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region5/ AUTO_COMPRESS=FALSE;

-- Region 6 (2 files)
PUT 'file://data/Region 6/customers_6.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region6/ AUTO_COMPRESS=FALSE;
PUT 'file://data/Region 6/returns_1.json'  @GLOBALMART.RAW.SOURCE_DATA_STAGE/Region6/ AUTO_COMPRESS=FALSE;

-- Root-level files (4 files)
PUT 'file://data/transactions_3.csv' @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
PUT 'file://data/returns_2.json'     @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
PUT 'file://data/products.json'      @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;
PUT 'file://data/vendors.csv'        @GLOBALMART.RAW.SOURCE_DATA_STAGE/ AUTO_COMPRESS=FALSE;

-- Verify upload
LIST @GLOBALMART.RAW.SOURCE_DATA_STAGE;
