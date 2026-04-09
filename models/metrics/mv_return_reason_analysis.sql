{{ config(materialized='view') }}

SELECT
    r.return_reason,
    c.region,
    COUNT(*) AS return_count,
    ROUND(SUM(r.refund_amount), 2) AS total_refunded,
    ROUND(AVG(r.refund_amount), 2) AS avg_refund,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    SUM(CASE WHEN r.return_status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN r.return_status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_count
FROM {{ ref('fact_returns') }} r
INNER JOIN {{ ref('dim_customers') }} c ON r.customer_sk = c.customer_sk
WHERE r.customer_sk IS NOT NULL
GROUP BY r.return_reason, c.region
