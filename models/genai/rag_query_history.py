import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType


def model(dbt, session: snowpark.Session):
    """
    UC3: Product Intelligence RAG — Snowpark Python model.
    1. Builds document chunks from Gold/Metrics tables
    2. Stores in RAG_DOCUMENTS table (auto-created if missing)
    3. Retrieves relevant docs via Cortex Search or keyword fallback
    4. Calls CORTEX.COMPLETE() with retrieved context
    5. Returns query history dataframe
    """
    dbt.config(
        materialized="table",
        schema="GOLD",
        packages=["snowflake-snowpark-python"]
    )

    # ── Step 1: Build document chunks ──

    product_docs = session.sql("""
        SELECT
            'product_' || product_id AS doc_id,
            'product_summary' AS doc_type,
            'Product: ' || product_name || '. Brand: ' || COALESCE(brand, 'Unknown')
            || '. Category: ' || COALESCE(category, 'Unknown')
            || '. UPC: ' || COALESCE(upc, 'N/A') || '.' AS doc_text,
            product_id,
            CAST(NULL AS VARCHAR) AS vendor_id,
            CAST(NULL AS VARCHAR) AS region,
            CURRENT_TIMESTAMP() AS updated_at
        FROM GLOBALMART.GOLD.DIM_PRODUCTS
    """)

    product_region_docs = session.sql("""
        SELECT
            'prodregion_' || product_id || '_' || region AS doc_id,
            'product_region' AS doc_type,
            'Product: ' || product_name || ' in ' || region || ' region. '
            || 'Total sales: $' || total_sales::VARCHAR || '. '
            || 'Quantity sold: ' || total_quantity::VARCHAR || '. '
            || 'Orders: ' || order_count::VARCHAR || '. '
            || 'Profit: $' || total_profit::VARCHAR || '. '
            || CASE WHEN is_slow_moving THEN 'This product is SLOW-MOVING in this region.' ELSE '' END
            AS doc_text,
            product_id,
            CAST(NULL AS VARCHAR) AS vendor_id,
            region,
            CURRENT_TIMESTAMP() AS updated_at
        FROM GLOBALMART.GOLD.MV_SLOW_MOVING_PRODUCTS
    """)

    vendor_docs = session.sql("""
        SELECT
            'vendor_' || vendor_id AS doc_id,
            'vendor_summary' AS doc_type,
            'Vendor: ' || vendor_name || '. '
            || 'Total orders: ' || total_orders::VARCHAR || '. '
            || 'Total sales: $' || total_sales::VARCHAR || '. '
            || 'Returns: ' || return_order_count::VARCHAR || '. '
            || 'Return rate: ' || return_rate_pct::VARCHAR || '%. '
            || 'Total refunded: $' || total_refunded::VARCHAR || '.'
            AS doc_text,
            CAST(NULL AS VARCHAR) AS product_id,
            vendor_id,
            CAST(NULL AS VARCHAR) AS region,
            CURRENT_TIMESTAMP() AS updated_at
        FROM GLOBALMART.GOLD.MV_RETURN_RATE_BY_VENDOR
    """)

    # ── Step 2: Store documents in RAG_DOCUMENTS ──

    # Create table if it doesn't exist (setup/07 may not have been run)
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

    # Union all document types and overwrite
    all_docs = product_docs.union_all(product_region_docs).union_all(vendor_docs)
    all_docs.write.mode("overwrite").save_as_table("GLOBALMART.GOLD.RAG_DOCUMENTS")

    # ── Step 3: Query with 5 test questions ──

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
            # Retrieve relevant docs via keyword search on RAG_DOCUMENTS
            keywords = [w for w in question.replace("?", "").split() if len(w) > 3]
            like_clauses = " OR ".join(
                [f"doc_text ILIKE '%{kw}%'" for kw in keywords[:5]]
            )
            search_result = session.sql(f"""
                SELECT doc_text FROM GLOBALMART.GOLD.RAG_DOCUMENTS
                WHERE {like_clauses}
                LIMIT 5
            """).collect()
            retrieved_docs = " | ".join([str(r[0]) for r in search_result])
            retrieved_count = len(search_result)

            # Call CORTEX.COMPLETE with retrieved context
            safe_q = question.replace("'", "''")
            safe_docs = retrieved_docs[:3000].replace("'", "''")
            answer_result = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3.1-70b',
                    'Answer ONLY from the retrieved documents below. '
                    || 'If the information is not in the documents, say so. '
                    || 'Question: {safe_q} '
                    || 'Documents: {safe_docs}'
                ) AS answer
            """).collect()

            answer = str(answer_result[0][0]) if answer_result else "No answer generated."
            results.append((question, answer, retrieved_docs[:4000], retrieved_count, 0.0))
        except Exception as e:
            results.append((question, f"Error: {str(e)}", "", 0, 0.0))

    # ── Step 4: Return results as DataFrame ──
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
