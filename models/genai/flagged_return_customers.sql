{{ config(materialized='table', schema='GOLD') }}

WITH profiles AS (
    SELECT * FROM {{ ref('mv_customer_return_history') }}
),

thresholds AS (
    SELECT
        GREATEST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_returns), 3) AS p75_returns,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_refund_value) AS p75_refund,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_refund_per_return) AS p75_avg_refund
    FROM profiles
),

scored AS (
    SELECT
        p.*,
        -- 5 weighted anomaly rules (pure SQL)
        CASE WHEN p.total_returns > t.p75_returns THEN 20 ELSE 0 END AS score_high_return_count,
        CASE WHEN p.total_refund_value > t.p75_refund THEN 25 ELSE 0 END AS score_high_total_refund,
        CASE WHEN p.avg_refund_per_return > t.p75_avg_refund THEN 20 ELSE 0 END AS score_high_avg_refund,
        CASE WHEN p.approval_rate_pct > 90 AND p.total_returns >= 2 THEN 15 ELSE 0 END AS score_high_approval_rate,
        CASE WHEN p.distinct_reason_count >= 3 THEN 20 ELSE 0 END AS score_multiple_reasons
    FROM profiles p
    CROSS JOIN thresholds t
),

with_totals AS (
    SELECT
        *,
        score_high_return_count + score_high_total_refund + score_high_avg_refund
            + score_high_approval_rate + score_multiple_reasons AS anomaly_score,
        ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN score_high_return_count > 0 THEN 'high_return_count (20)' END,
            CASE WHEN score_high_total_refund > 0 THEN 'high_total_refund (25)' END,
            CASE WHEN score_high_avg_refund > 0 THEN 'high_avg_refund (20)' END,
            CASE WHEN score_high_approval_rate > 0 THEN 'high_approval_rate (15)' END,
            CASE WHEN score_multiple_reasons > 0 THEN 'multiple_reasons (20)' END
        ), ', ') AS rules_violated
    FROM scored
),

flagged AS (
    SELECT * FROM with_totals WHERE anomaly_score >= 40
)

SELECT
    customer_id,
    customer_name,
    segment,
    region,
    total_returns,
    total_refund_value,
    avg_refund_per_return,
    approval_rate_pct AS approval_rate,
    distinct_reason_count,
    return_reasons,
    anomaly_score,
    rules_violated,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are a fraud investigator for GlobalMart. Write a 4-5 sentence investigation brief. '
        || 'Cite actual numbers. Acknowledge one innocent explanation. '
        || 'Identify the strongest fraud signal. Recommend exactly 2 specific next actions. '
        || 'Customer: ' || customer_name || ' (' || customer_id || '). '
        || 'Segment: ' || segment || '. Region: ' || region || '. '
        || 'Returns: ' || total_returns::VARCHAR || '. '
        || 'Total refunded: $' || total_refund_value::VARCHAR || '. '
        || 'Avg refund: $' || avg_refund_per_return::VARCHAR || '. '
        || 'Approval rate: ' || approval_rate_pct::VARCHAR || '%. '
        || 'Reasons: ' || COALESCE(return_reasons, 'N/A') || '. '
        || 'Score: ' || anomaly_score::VARCHAR || '/100. '
        || 'Rules: ' || rules_violated,
        {'temperature': 0.3, 'max_tokens': 512}
    ) AS investigation_brief,
    CURRENT_TIMESTAMP() AS generated_at
FROM flagged
