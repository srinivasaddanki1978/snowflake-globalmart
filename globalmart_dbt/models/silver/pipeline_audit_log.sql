{{ config(materialized='view') }}

SELECT 'customers' AS entity,
    (SELECT COUNT(*) FROM {{ ref('raw_customers') }}) AS bronze_count,
    (SELECT COUNT(*) FROM {{ ref('clean_customers') }}) AS silver_count,
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_customers') AS rejected_count
UNION ALL
SELECT 'orders',
    (SELECT COUNT(*) FROM {{ ref('raw_orders') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_orders') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_orders')
UNION ALL
SELECT 'transactions',
    (SELECT COUNT(*) FROM {{ ref('raw_transactions') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_transactions') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_transactions')
UNION ALL
SELECT 'returns',
    (SELECT COUNT(*) FROM {{ ref('raw_returns') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_returns') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_returns')
UNION ALL
SELECT 'products',
    (SELECT COUNT(*) FROM {{ ref('raw_products') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_products') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_products')
UNION ALL
SELECT 'vendors',
    (SELECT COUNT(*) FROM {{ ref('raw_vendors') }}),
    (SELECT COUNT(*) FROM {{ ref('clean_vendors') }}),
    (SELECT COUNT(*) FROM {{ ref('rejected_records') }} WHERE source_table = 'clean_vendors')
