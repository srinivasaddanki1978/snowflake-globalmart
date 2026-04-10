-- ============================================================
-- GlobalMart: Step 7 — GenAI Infrastructure
-- RAG_DOCUMENTS table + Cortex Search Service
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GLOBALMART;
USE SCHEMA GOLD;
USE WAREHOUSE COMPUTE_WH;

-- Create documents table for Cortex Search Service
CREATE TABLE IF NOT EXISTS GLOBALMART.GOLD.RAG_DOCUMENTS (
    doc_id VARCHAR,
    doc_type VARCHAR,
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
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
AS (
    SELECT doc_id, doc_type, doc_text, product_id, vendor_id, region, updated_at
    FROM GLOBALMART.GOLD.RAG_DOCUMENTS
);

-- ============================================================
-- Cortex Analyst: Semantic Model Stage
-- Upload semantic_model.yaml via deploy.py or manually:
--   PUT 'file://cortex_analyst/semantic_model.yaml'
--       @GLOBALMART.GOLD.SEMANTIC_MODEL_STAGE
--       AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- ============================================================

CREATE STAGE IF NOT EXISTS GLOBALMART.GOLD.SEMANTIC_MODEL_STAGE
  COMMENT = 'Stores Cortex Analyst semantic model YAML';
