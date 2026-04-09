{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    upc,
    _source_file,
    _load_timestamp
FROM {{ ref('_products_harmonized') }}
WHERE NOT _has_any_failure
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
