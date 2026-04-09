{{ config(materialized='table') }}

-- Bridge grain: (order_id x product_sk) — one row per returned order per product.
-- Pre-aggregates fact_sales and fact_returns to be self-healing against any
-- upstream duplication from incremental/append strategy.

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

order_totals AS (
    SELECT
        order_id,
        SUM(sales) AS total_order_sales
    FROM sales_agg
    GROUP BY order_id
)

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
    ot.total_order_sales,
    ROUND(s.sales / NULLIF(ot.total_order_sales, 0), 4) AS allocation_weight,
    ROUND(r.refund_amount * (s.sales / NULLIF(ot.total_order_sales, 0)), 2) AS allocated_refund,
    r._load_timestamp
FROM returns_agg r
INNER JOIN sales_agg s ON r.order_id = s.order_id
INNER JOIN order_totals ot ON r.order_id = ot.order_id
