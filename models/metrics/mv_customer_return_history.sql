{{ config(materialized='view') }}

SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    c.region,
    COUNT(*) AS total_returns,
    ROUND(SUM(r.refund_amount), 2) AS total_refund_value,
    ROUND(AVG(r.refund_amount), 2) AS avg_refund_per_return,
    COUNT(DISTINCT r.return_reason) AS distinct_reason_count,
    LISTAGG(DISTINCT r.return_reason, ', ') WITHIN GROUP (ORDER BY r.return_reason) AS return_reasons,
    SUM(CASE WHEN r.return_status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    ROUND(SUM(CASE WHEN r.return_status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS approval_rate_pct,
    MIN(r.return_date) AS first_return_date,
    MAX(r.return_date) AS last_return_date
FROM {{ ref('fact_returns') }} r
INNER JOIN {{ ref('dim_customers') }} c ON r.customer_sk = c.customer_sk
WHERE r.customer_sk IS NOT NULL
GROUP BY c.customer_id, c.customer_name, c.segment, c.region
