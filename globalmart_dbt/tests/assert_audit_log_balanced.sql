-- Singular test: Verify bronze_count = silver_count + rejected_count for each entity
-- Returns failing rows (should return 0 rows for test to pass)

SELECT
    entity,
    bronze_count,
    silver_count,
    rejected_count,
    bronze_count - (silver_count + rejected_count) AS imbalance
FROM {{ ref('pipeline_audit_log') }}
WHERE bronze_count != (silver_count + rejected_count)
