{{ config(materialized='table') }}

WITH product_region_sales AS (
    SELECT
        s.product_id,
        p.product_name,
        p.brand,
        c.region,
        ROUND(SUM(s.sales), 2) AS total_sales,
        SUM(s.quantity) AS total_quantity,
        COUNT(DISTINCT s.order_id) AS order_count,
        ROUND(SUM(s.profit), 2) AS total_profit
    FROM {{ ref('fact_sales') }} s
    INNER JOIN {{ ref('dim_products') }} p ON s.product_sk = p.product_sk
    INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
    GROUP BY s.product_id, p.product_name, p.brand, c.region
)

SELECT
    *,
    CASE
        WHEN PERCENT_RANK() OVER (PARTITION BY region ORDER BY total_quantity) <= 0.25
        THEN TRUE ELSE FALSE
    END AS is_slow_moving
FROM product_region_sales
