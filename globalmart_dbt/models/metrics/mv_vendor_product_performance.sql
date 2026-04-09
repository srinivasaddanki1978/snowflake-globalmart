{{ config(materialized='view') }}

WITH sales_agg AS (
    SELECT
        v.vendor_id,
        v.vendor_name,
        p.product_id,
        p.product_name,
        p.brand,
        c.region,
        COUNT(DISTINCT s.order_id) AS order_count,
        ROUND(SUM(s.sales), 2) AS total_sales,
        SUM(s.quantity) AS total_quantity,
        ROUND(SUM(s.profit), 2) AS total_profit
    FROM {{ ref('fact_sales') }} s
    INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
    INNER JOIN {{ ref('dim_products') }} p ON s.product_sk = p.product_sk
    INNER JOIN {{ ref('dim_vendors') }} v ON s.vendor_sk = v.vendor_sk
    GROUP BY v.vendor_id, v.vendor_name, p.product_id, p.product_name, p.brand, c.region
),

return_agg AS (
    SELECT
        s.vendor_id,
        b.product_id,
        COUNT(*) AS return_count,
        ROUND(SUM(b.allocated_refund), 2) AS total_allocated_refund
    FROM {{ ref('bridge_return_products') }} b
    INNER JOIN (
        SELECT DISTINCT order_id, vendor_id
        FROM {{ ref('fact_sales') }}
    ) s ON b.order_id = s.order_id
    GROUP BY s.vendor_id, b.product_id
)

SELECT
    sa.vendor_id,
    sa.vendor_name,
    sa.product_id,
    sa.product_name,
    sa.brand,
    sa.region,
    sa.order_count,
    sa.total_sales,
    sa.total_quantity,
    sa.total_profit,
    COALESCE(ra.return_count, 0) AS return_count,
    COALESCE(ra.total_allocated_refund, 0.0) AS total_allocated_refund
FROM sales_agg sa
LEFT JOIN return_agg ra ON sa.vendor_id = ra.vendor_id AND sa.product_id = ra.product_id
