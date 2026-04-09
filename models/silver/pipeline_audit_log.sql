{{ config(materialized='view') }}

-- Use harmonized models to count rejected records (1 row per record, not per rule)

SELECT 'customers' AS entity,
    (SELECT COUNT(*) FROM {{ ref('raw_customers') }}) AS bronze_count,
    (SELECT COUNT(*) FROM {{ ref('clean_customers') }}) AS silver_count,
    (SELECT COUNT(*) FROM {{ ref('_customers_harmonized') }} WHERE _has_any_failure) AS rejected_count
UNION ALL
SELECT 'orders',
    (SELECT COUNT(*) FROM {{ ref('raw_orders') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_orders') }}),
    (SELECT COUNT(*) FROM {{ ref('_orders_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'transactions',
    (SELECT COUNT(*) FROM {{ ref('raw_transactions') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_transactions') }}),
    (SELECT COUNT(*) FROM {{ ref('_transactions_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'returns',
    (SELECT COUNT(*) FROM {{ ref('raw_returns') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_returns') }}),
    (SELECT COUNT(*) FROM {{ ref('_returns_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'products',
    (SELECT COUNT(*) FROM {{ ref('raw_products') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_products') }}),
    (SELECT COUNT(*) FROM {{ ref('_products_harmonized') }} WHERE _has_any_failure)
UNION ALL
SELECT 'vendors',
    (SELECT COUNT(*) FROM {{ ref('raw_vendors') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_vendors') }}),
    (SELECT COUNT(*) FROM {{ ref('_vendors_harmonized') }} WHERE _has_any_failure)
