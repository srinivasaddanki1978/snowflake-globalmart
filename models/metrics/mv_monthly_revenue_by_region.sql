{{ config(materialized='view') }}

SELECT
    d.year,
    d.month,
    d.month_name,
    c.region,
    COUNT(DISTINCT s.order_id) AS order_count,
    ROUND(SUM(s.sales), 2) AS total_revenue,
    ROUND(SUM(s.profit), 2) AS total_profit,
    SUM(s.quantity) AS total_quantity,
    COUNT(DISTINCT s.customer_id) AS unique_customers,
    ROUND(AVG(s.sales), 2) AS avg_order_value
FROM {{ ref('fact_sales') }} s
INNER JOIN {{ ref('dim_customers') }} c ON s.customer_sk = c.customer_sk
INNER JOIN {{ ref('dim_dates') }} d ON s.order_date_sk = d.date_sk
GROUP BY d.year, d.month, d.month_name, c.region
