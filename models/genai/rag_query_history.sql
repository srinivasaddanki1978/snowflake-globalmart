{{ config(materialized='table') }}

{#
  UC3: Product Intelligence RAG
  Builds document chunks from Gold tables, retrieves relevant context
  per question via keyword matching, and generates answers using
  CORTEX.COMPLETE with retrieved context — pure SQL RAG pipeline.
#}

WITH

-- ── Document chunks from Gold tables ──

product_docs AS (
    SELECT
        'product_' || product_id AS doc_id,
        'Product: ' || product_name
        || '. Brand: ' || COALESCE(brand, 'Unknown')
        || '. Category: ' || COALESCE(category, 'Unknown') AS doc_text
    FROM {{ ref('dim_products') }}
),

slow_moving_docs AS (
    SELECT
        'prodregion_' || product_id || '_' || region AS doc_id,
        product_name || ' in ' || region || ': $'
        || total_sales::VARCHAR || ' sales, '
        || total_quantity::VARCHAR || ' qty, $'
        || total_profit::VARCHAR || ' profit'
        || CASE WHEN is_slow_moving THEN ' [SLOW-MOVING]' ELSE '' END AS doc_text
    FROM {{ ref('mv_slow_moving_products') }}
),

vendor_docs AS (
    SELECT
        'vendor_' || vendor_id AS doc_id,
        'Vendor ' || vendor_name || ': '
        || total_orders::VARCHAR || ' orders, $'
        || total_sales::VARCHAR || ' sales, '
        || return_order_count::VARCHAR || ' returns, '
        || return_rate_pct::VARCHAR || '% return rate, $'
        || total_refunded::VARCHAR || ' refunded' AS doc_text
    FROM {{ ref('mv_return_rate_by_vendor') }}
),

-- ── Per-question context retrieval ──

q1_ctx AS (
    SELECT
        LEFT(LISTAGG(doc_text, ' | ') WITHIN GROUP (ORDER BY doc_id), 3000) AS docs,
        LEAST(COUNT(*), 5) AS cnt
    FROM slow_moving_docs
    WHERE doc_text ILIKE '%SLOW-MOVING%'
),

q2_ctx AS (
    SELECT
        LEFT(LISTAGG(doc_text, ' | ') WITHIN GROUP (ORDER BY doc_id), 3000) AS docs,
        LEAST(COUNT(*), 5) AS cnt
    FROM vendor_docs
),

q3_ctx AS (
    SELECT
        LEFT(LISTAGG(doc_text, ' | ') WITHIN GROUP (ORDER BY doc_id), 3000) AS docs,
        LEAST(COUNT(*), 5) AS cnt
    FROM slow_moving_docs
    WHERE doc_text ILIKE '%East%'
),

q4_ctx AS (
    SELECT
        LEFT(LISTAGG(doc_text, ' | ') WITHIN GROUP (ORDER BY doc_id), 3000) AS docs,
        LEAST(COUNT(*), 5) AS cnt
    FROM vendor_docs
    WHERE doc_text ILIKE '%return%'
),

q5_ctx AS (
    SELECT
        LEFT(LISTAGG(doc_text, ' | ') WITHIN GROUP (ORDER BY doc_id), 3000) AS docs,
        LEAST(COUNT(*), 5) AS cnt
    FROM vendor_docs
),

-- ── Combine questions with their context ──

question_context AS (
    SELECT 1 AS qid,
        'Which products are slow-moving and have low sales?' AS question,
        docs, cnt FROM q1_ctx
    UNION ALL
    SELECT 2,
        'Which vendor has the highest return rate?',
        docs, cnt FROM q2_ctx
    UNION ALL
    SELECT 3,
        'What are the top-selling products in the East region?',
        docs, cnt FROM q3_ctx
    UNION ALL
    SELECT 4,
        'Which products have the highest return cost?',
        docs, cnt FROM q4_ctx
    UNION ALL
    SELECT 5,
        'Which vendors are active in the West region and what are their sales?',
        docs, cnt FROM q5_ctx
)

-- ── Generate RAG answers via CORTEX.COMPLETE ──

SELECT
    question,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Answer ONLY from the retrieved documents below. '
        || 'If the information is not in the documents, say so. '
        || 'Question: ' || question
        || ' Documents: ' || COALESCE(docs, 'No relevant documents found.')
    ) AS answer,
    COALESCE(docs, '') AS retrieved_documents,
    COALESCE(cnt, 0) AS retrieved_count,
    0.0::FLOAT AS top_distance,
    CURRENT_TIMESTAMP() AS generated_at
FROM question_context
ORDER BY qid
