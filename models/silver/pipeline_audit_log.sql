{{ config(materialized='view') }}

-- Reconciliation view: all counts derived from harmonized models (ephemeral,
-- recomputed fresh from Bronze every run). Validates that _has_any_failure
-- flag properly partitions all records into pass/fail without loss.
-- Guarantees: bronze_count = silver_count + rejected_count

SELECT 'customers' AS entity,
    (SELECT COUNT(*) FROM {{ ref('_customers_harmonized') }}) AS bronze_count,
    (SELECT COUNT(*) FROM {{ ref('_customers_harmonized') }} WHERE NOT _has_any_failure) AS silver_count,
    (SELECT COUNT(*) FROM {{ ref('_customers_harmonized') }} WHERE _has_any_failure) AS rejected_count
UNION ALL
SELECT 'orders',
    (SELECT COUNT(*) FROM {{ ref('_orders_harmonized') }}),
    (SELECT COUNT(*) FROM {{ ref('_orders_harmonized') }} WHERE NOT _has_any_failure),
    (SELECT COUNT(*) FROM {{ ref('_orders_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'transactions',
    (SELECT COUNT(*) FROM {{ ref('_transactions_harmonized') }}),
    (SELECT COUNT(*) FROM {{ ref('_transactions_harmonized') }} WHERE NOT _has_any_failure),
    (SELECT COUNT(*) FROM {{ ref('_transactions_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'returns',
    (SELECT COUNT(*) FROM {{ ref('_returns_harmonized') }}),
    (SELECT COUNT(*) FROM {{ ref('_returns_harmonized') }} WHERE NOT _has_any_failure),
    (SELECT COUNT(*) FROM {{ ref('_returns_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'products',
    (SELECT COUNT(*) FROM {{ ref('_products_harmonized') }}),
    (SELECT COUNT(*) FROM {{ ref('_products_harmonized') }} WHERE NOT _has_any_failure),
    (SELECT COUNT(*) FROM {{ ref('_products_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'vendors',
    (SELECT COUNT(*) FROM {{ ref('_vendors_harmonized') }}),
    (SELECT COUNT(*) FROM {{ ref('_vendors_harmonized') }} WHERE NOT _has_any_failure),
    (SELECT COUNT(*) FROM {{ ref('_vendors_harmonized') }} WHERE _has_any_failure)
