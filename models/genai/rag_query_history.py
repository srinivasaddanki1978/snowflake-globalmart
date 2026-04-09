import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType


def model(dbt, session: snowpark.Session):
    """
    UC3: Product Intelligence RAG — Snowpark Python model.
    1. Builds document chunks from Gold/Metrics tables
    2. Stores in RAG_DOCUMENTS table (auto-created if missing)
    3. Retrieves relevant docs via keyword search
    4. Calls CORTEX.COMPLETE() with retrieved context
    5. Returns query history dataframe
    """
    dbt.config(
        materialized="table",
        schema="GOLD",
        packages=["snowflake-snowpark-python"]
    )

    # ── Step 1: Create RAG_DOCUMENTS table if missing ──
    session.sql("""
        CREATE TABLE IF NOT EXISTS GLOBALMART.GOLD.RAG_DOCUMENTS (
            doc_id VARCHAR,
            doc_type VARCHAR,
            doc_text VARCHAR,
            product_id VARCHAR,
            vendor_id VARCHAR,
            region VARCHAR,
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    session.sql("TRUNCATE TABLE IF EXISTS GLOBALMART.GOLD.RAG_DOCUMENTS").collect()

    # ── Step 2: Insert product summaries ──
    session.sql("""
        INSERT INTO GLOBALMART.GOLD.RAG_DOCUMENTS (doc_id, doc_type, doc_text, product_id, vendor_id, region, updated_at)
        SELECT
            'product_' || product_id,
            'product_summary',
            'Product: ' || product_name || '. Brand: ' || COALESCE(brand, 'Unknown')
            || '. Category: ' || COALESCE(category, 'Unknown')
            || '. UPC: ' || COALESCE(upc, 'N/A') || '.',
            product_id,
            NULL,
            NULL,
            CURRENT_TIMESTAMP()
        FROM GLOBALMART.GOLD.DIM_PRODUCTS
    """).collect()

    # ── Step 3: Insert product-region summaries ──
    session.sql("""
        INSERT INTO GLOBALMART.GOLD.RAG_DOCUMENTS (doc_id, doc_type, doc_text, product_id, vendor_id, region, updated_at)
        SELECT
            'prodregion_' || product_id || '_' || region,
            'product_region',
            'Product: ' || product_name || ' in ' || region || ' region. '
            || 'Total sales: $' || total_sales::VARCHAR || '. '
            || 'Quantity sold: ' || total_quantity::VARCHAR || '. '
            || 'Orders: ' || order_count::VARCHAR || '. '
            || 'Profit: $' || total_profit::VARCHAR || '. '
            || CASE WHEN is_slow_moving THEN 'This product is SLOW-MOVING in this region.' ELSE '' END,
            product_id,
            NULL,
            region,
            CURRENT_TIMESTAMP()
        FROM GLOBALMART.GOLD.MV_SLOW_MOVING_PRODUCTS
    """).collect()

    # ── Step 4: Insert vendor summaries ──
    session.sql("""
        INSERT INTO GLOBALMART.GOLD.RAG_DOCUMENTS (doc_id, doc_type, doc_text, product_id, vendor_id, region, updated_at)
        SELECT
            'vendor_' || vendor_id,
            'vendor_summary',
            'Vendor: ' || vendor_name || '. '
            || 'Total orders: ' || total_orders::VARCHAR || '. '
            || 'Total sales: $' || total_sales::VARCHAR || '. '
            || 'Returns: ' || return_order_count::VARCHAR || '. '
            || 'Return rate: ' || return_rate_pct::VARCHAR || '%. '
            || 'Total refunded: $' || total_refunded::VARCHAR || '.',
            NULL,
            vendor_id,
            NULL,
            CURRENT_TIMESTAMP()
        FROM GLOBALMART.GOLD.MV_RETURN_RATE_BY_VENDOR
    """).collect()

    # ── Step 5: Query with 5 test questions ──
    test_questions = [
        "Which products are slow-moving and have low sales?",
        "Which vendor has the highest return rate?",
        "What are the top-selling products in the East region?",
        "Which products have the highest return cost?",
        "Which vendors are active in the West region and what are their sales?"
    ]

    results = []
    for question in test_questions:
        try:
            # Keyword search on RAG_DOCUMENTS
            keywords = [w for w in question.replace("?", "").split() if len(w) > 3]
            like_clauses = " OR ".join(
                ["doc_text ILIKE '%" + kw + "%'" for kw in keywords[:5]]
            )
            search_rows = session.sql(
                "SELECT doc_text FROM GLOBALMART.GOLD.RAG_DOCUMENTS"
                + " WHERE " + like_clauses
                + " LIMIT 5"
            ).collect()
            retrieved_docs = " | ".join([str(r[0]) for r in search_rows])
            retrieved_count = len(search_rows)

            # Call CORTEX.COMPLETE with retrieved context
            safe_q = question.replace("'", "''")
            safe_docs = retrieved_docs[:3000].replace("'", "''")
            prompt = (
                "Answer ONLY from the retrieved documents below. "
                "If the information is not in the documents, say so. "
                "Question: " + safe_q + " "
                "Documents: " + safe_docs
            )
            answer_rows = session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '"
                + prompt.replace("'", "''") + "') AS answer"
            ).collect()

            answer = str(answer_rows[0][0]) if answer_rows else "No answer generated."
            results.append((question, answer, retrieved_docs[:4000], retrieved_count, 0.0))
        except Exception as e:
            results.append((question, "Error: " + str(e), "", 0, 0.0))

    # ── Step 6: Return results as DataFrame ──
    schema = StructType([
        StructField("question", StringType()),
        StructField("answer", StringType()),
        StructField("retrieved_documents", StringType()),
        StructField("retrieved_count", IntegerType()),
        StructField("top_distance", FloatType()),
    ])

    result_df = session.create_dataframe(results, schema=schema)
    result_df = result_df.with_column("generated_at", current_timestamp())

    return result_df
