{{ config(materialized='view') }}

SELECT
    c.segment,
    c.region,
    COUNT(DISTINCT s.order_id) AS order_count,
    COUNT(DISTINCT s.customer_id) AS customer_count,
    ROUND(SUM(s.sales), 2) AS total_revenue,
    ROUND(SUM(s.profit), 2) AS total_profit,
    ROUND(SUM(s.profit) * 100.0 / NULLIF(SUM(s.sales), 0), 2) AS profit_margin_pct,
    ROUND(AVG(s.discount), 4) AS avg_discount
FROM {{ ref('fact_sales') }} s
INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
GROUP BY c.segment, c.region
