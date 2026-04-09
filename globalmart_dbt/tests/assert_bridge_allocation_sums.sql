-- Singular test: Verify allocation_weight sums to ~1.0 per order in bridge_return_products
-- Allows 0.01 tolerance for rounding
-- Returns failing rows (should return 0 rows for test to pass)

SELECT
    order_id,
    ROUND(SUM(allocation_weight), 4) AS total_weight,
    ABS(SUM(allocation_weight) - 1.0) AS deviation
FROM {{ ref('bridge_return_products') }}
GROUP BY order_id
HAVING ABS(SUM(allocation_weight) - 1.0) > 0.01
