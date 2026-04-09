import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, current_timestamp, call_udf
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from datetime import datetime


def model(dbt, session: snowpark.Session):
    """
    UC3: Product Intelligence RAG — Snowpark Python model.
    1. Builds document chunks from Gold/Metrics tables
    2. Inserts into RAG_DOCUMENTS table
    3. Queries Cortex Search Service for 5 test questions
    4. Calls CORTEX.COMPLETE() with retrieved context
    5. Returns query history dataframe
    """
    dbt.config(
        materialized="table",
        schema="GOLD",
        packages=["snowflake-snowpark-python"]
    )

    # ── Step 1: Build document chunks ──

    # Product summaries (1 per product)
    product_docs = session.sql("""
        SELECT
            'product_' || product_id AS doc_id,
            'product_summary' AS doc_type,
            'Product: ' || product_name || '. Brand: ' || COALESCE(brand, 'Unknown')
            || '. Category: ' || COALESCE(category, 'Unknown')
            || '. UPC: ' || COALESCE(upc, 'N/A') || '.' AS doc_text,
            product_id,
            NULL AS vendor_id,
            NULL AS region,
            CURRENT_TIMESTAMP() AS updated_at
        FROM GLOBALMART.GOLD.DIM_PRODUCTS
    """)

    # Product-region summaries (1 per product x region)
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
            NULL AS vendor_id,
            region,
            CURRENT_TIMESTAMP() AS updated_at
        FROM GLOBALMART.GOLD.MV_SLOW_MOVING_PRODUCTS
    """)

    # Vendor summaries (1 per vendor)
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
            NULL AS product_id,
            vendor_id,
            NULL AS region,
            CURRENT_TIMESTAMP() AS updated_at
        FROM GLOBALMART.GOLD.MV_RETURN_RATE_BY_VENDOR
    """)

    # ── Step 2: Insert into RAG_DOCUMENTS ──
    session.sql("DELETE FROM GLOBALMART.GOLD.RAG_DOCUMENTS").collect()

    # Insert using INSERT INTO ... SELECT to match exact table schema
    product_docs.write.mode("append").save_as_table(
        "GLOBALMART.GOLD.RAG_DOCUMENTS",
        column_order="name"
    )
    product_region_docs.write.mode("append").save_as_table(
        "GLOBALMART.GOLD.RAG_DOCUMENTS",
        column_order="name"
    )
    vendor_docs.write.mode("append").save_as_table(
        "GLOBALMART.GOLD.RAG_DOCUMENTS",
        column_order="name"
    )

    # ── Step 3: Query Cortex Search Service with 5 test questions ──
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
            # Use Cortex Search Service to retrieve relevant docs
            search_result = session.sql(f"""
                SELECT
                    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                        'GLOBALMART.GOLD.PRODUCT_SEARCH_SERVICE',
                        '{question.replace("'", "''")}',
                        5
                    ) AS search_results
            """).collect()

            if search_result and len(search_result) > 0:
                retrieved_docs = str(search_result[0][0])
                retrieved_count = min(5, retrieved_docs.count("doc_id"))
            else:
                retrieved_docs = "No documents retrieved."
                retrieved_count = 0

            # Call CORTEX.COMPLETE with retrieved context
            answer_result = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3.1-70b',
                    'Answer ONLY from the retrieved documents below. '
                    || 'If the information is not in the documents, say so. '
                    || 'Question: {question.replace("'", "''")} '
                    || 'Documents: {retrieved_docs[:3000].replace("'", "''")}'
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
