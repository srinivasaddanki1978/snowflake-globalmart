{{ config(materialized='incremental', incremental_strategy='append') }}

WITH returns AS (
    SELECT * FROM {{ ref('fact_returns') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
    {% endif %}
),

sales AS (
    SELECT * FROM {{ ref('fact_sales') }}
),

order_totals AS (
    SELECT
        order_id,
        SUM(sales) AS total_order_sales
    FROM sales
    GROUP BY order_id
),

joined AS (
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
        s.sales / NULLIF(ot.total_order_sales, 0) AS allocation_weight,
        r.refund_amount * (s.sales / NULLIF(ot.total_order_sales, 0)) AS allocated_refund,
        r._load_timestamp
    FROM returns r
    INNER JOIN sales s ON r.order_id = s.order_id
    INNER JOIN order_totals ot ON r.order_id = ot.order_id
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
    total_order_sales,
    ROUND(allocation_weight, 4) AS allocation_weight,
    ROUND(allocated_refund, 2) AS allocated_refund,
    _load_timestamp
FROM joined
