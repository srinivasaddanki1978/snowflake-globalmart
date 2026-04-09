{{ config(materialized='table') }}

WITH vendor_sales AS (
    SELECT
        vendor_id,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(sales), 2) AS total_sales
    FROM {{ ref('fact_sales') }}
    WHERE vendor_id IS NOT NULL
    GROUP BY vendor_id
),

vendor_returns AS (
    SELECT
        vendor_id,
        COUNT(DISTINCT order_id) AS return_order_count,
        ROUND(SUM(refund_amount), 2) AS total_refunded
    FROM {{ ref('fact_returns') }}
    WHERE vendor_id IS NOT NULL
    GROUP BY vendor_id
)

SELECT
    vs.vendor_id,
    v.vendor_name,
    vs.total_orders,
    vs.total_sales,
    COALESCE(vr.return_order_count, 0) AS return_order_count,
    COALESCE(vr.total_refunded, 0.0) AS total_refunded,
    ROUND(COALESCE(vr.return_order_count, 0) * 100.0 / NULLIF(vs.total_orders, 0), 2) AS return_rate_pct
FROM vendor_sales vs
INNER JOIN {{ ref('dim_vendors') }} v ON vs.vendor_id = v.vendor_id
LEFT JOIN vendor_returns vr ON vs.vendor_id = vr.vendor_id
