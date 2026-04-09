{{ config(materialized='table') }}

-- GROUP BY product_id guarantees one row per product (bulletproof dedup
-- regardless of upstream duplication from incremental/append runs)
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    product_id,
    MAX(product_name) AS product_name,
    MAX(brand) AS brand,
    MAX(category) AS category,
    MAX(sub_category) AS sub_category,
    MAX(upc) AS upc
FROM {{ ref('clean_products') }}
WHERE product_id IS NOT NULL
GROUP BY product_id
