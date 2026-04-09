{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    upc
FROM {{ ref('clean_products') }}
