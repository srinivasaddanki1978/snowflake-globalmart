{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY _load_timestamp DESC
        ) AS row_num
    FROM {{ ref('clean_products') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    upc
FROM ranked
WHERE row_num = 1
