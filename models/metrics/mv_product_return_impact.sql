{{ config(materialized='view') }}

WITH sales_with_region AS (
    SELECT DISTINCT
        s.order_id,
        s.product_sk,
        c.region
    FROM {{ ref('fact_sales') }} s
    INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
)

SELECT
    p.product_id,
    p.product_name,
    p.brand,
    sr.region,
    COUNT(*) AS return_line_count,
    ROUND(SUM(b.allocated_refund), 2) AS total_return_cost,
    ROUND(AVG(b.allocated_refund), 2) AS avg_return_cost,
    ROUND(SUM(b.allocation_weight), 4) AS total_allocation_weight
FROM {{ ref('bridge_return_products') }} b
INNER JOIN sales_with_region sr ON b.order_id = sr.order_id AND b.product_sk = sr.product_sk
INNER JOIN {{ ref('dim_products') }} p ON b.product_sk = p.product_sk
GROUP BY p.product_id, p.product_name, p.brand, sr.region
