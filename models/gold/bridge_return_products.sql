{{ config(materialized='table') }}

-- Bridge grain: (order_id x product_sk) — one row per returned order per product.
-- Pre-aggregates fact_sales and fact_returns to self-heal against any upstream
-- duplication from incremental/append strategy.
--
-- CRITICAL: total_order_sales is computed via window function over the SAME
-- sales_agg rows that become bridge rows, guaranteeing allocation_weight sums
-- to exactly 1.0 per order_id (no ROUND, no cross-CTE JOIN mismatch).

WITH returns_agg AS (
    -- One row per order_id: sum refund across any multiple return events
    SELECT
        order_id,
        MAX(return_date_sk) AS return_date_sk,
        MAX(return_date_key) AS return_date_key,
        MAX(return_reason) AS return_reason,
        MAX(return_status) AS return_status,
        SUM(refund_amount) AS refund_amount,
        MAX(_load_timestamp) AS _load_timestamp
    FROM {{ ref('fact_returns') }}
    GROUP BY order_id
),

sales_agg AS (
    -- One row per (order_id, product_sk): sum any duplicated line items
    SELECT
        order_id,
        product_sk,
        MAX(product_id) AS product_id,
        SUM(sales) AS sales
    FROM {{ ref('fact_sales') }}
    GROUP BY order_id, product_sk
),

-- INNER JOIN to only keep products from orders that were actually returned
returned_sales AS (
    SELECT
        r.order_id,
        s.product_sk,
        s.product_id,
        r.return_date_sk,
        r.return_date_key,
        r.return_reason,
        r.return_status,
        r.refund_amount,
        s.sales AS line_sales,
        r._load_timestamp
    FROM returns_agg r
    INNER JOIN sales_agg s ON r.order_id = s.order_id
)

SELECT
    order_id,
    product_sk,
    product_id,
    return_date_sk,
    return_date_key,
    return_reason,
    return_status,
    refund_amount,
    line_sales,
    SUM(line_sales) OVER (PARTITION BY order_id) AS total_order_sales,
    -- Window function guarantees SUM(allocation_weight) = 1.0 per order_id
    line_sales / NULLIF(SUM(line_sales) OVER (PARTITION BY order_id), 0) AS allocation_weight,
    ROUND(refund_amount * (line_sales / NULLIF(SUM(line_sales) OVER (PARTITION BY order_id), 0)), 2) AS allocated_refund,
    _load_timestamp
FROM returned_sales
