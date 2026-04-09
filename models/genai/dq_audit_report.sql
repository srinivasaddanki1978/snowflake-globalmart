{{ config(materialized='table', schema='GOLD') }}

WITH rejection_groups AS (
    SELECT
        REPLACE(source_table, 'clean_', '') AS entity,
        affected_field,
        issue_type,
        COUNT(*) AS rejected_count,
        ARRAY_TO_STRING(ARRAY_SLICE(ARRAY_AGG(record_data), 0, 3), ' | ') AS sample_records
    FROM {{ ref('rejected_records') }}
    GROUP BY source_table, affected_field, issue_type
),

audit AS (
    SELECT entity, bronze_count AS total_entity_records
    FROM {{ ref('pipeline_audit_log') }}
),

enriched AS (
    SELECT
        rg.entity,
        rg.affected_field,
        rg.issue_type,
        rg.rejected_count,
        a.total_entity_records,
        ROUND(rg.rejected_count * 100.0 / NULLIF(a.total_entity_records, 0), 2) AS rejection_rate_pct,
        rg.sample_records
    FROM rejection_groups rg
    LEFT JOIN audit a ON rg.entity = a.entity
)

SELECT
    entity,
    affected_field,
    issue_type,
    rejected_count,
    total_entity_records,
    rejection_rate_pct,
    sample_records,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are a data quality analyst for GlobalMart, a US retail chain. '
        || 'Explain this data quality issue in 3-4 business-friendly sentences. '
        || 'Do NOT use technical jargon like NULL, parse failure, schema, or boolean. '
        || 'Name the specific field and sample values. '
        || 'Identify which business report is at risk. '
        || 'Recommend what the data steward should investigate. '
        || 'Entity: ' || entity
        || '. Field: ' || affected_field
        || '. Issue: ' || issue_type
        || '. Rejected: ' || rejected_count::VARCHAR
        || ' out of ' || COALESCE(total_entity_records::VARCHAR, 'unknown')
        || ' records (' || COALESCE(rejection_rate_pct::VARCHAR, 'N/A') || '%). '
        || 'Samples: ' || COALESCE(sample_records, 'none'),
        {'temperature': 0.3, 'max_tokens': 512}
    ) AS ai_explanation,
    CURRENT_TIMESTAMP() AS generated_at
FROM enriched
