{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"product_id"::STRING    AS product_id,
    raw_data:"product_name"::STRING  AS product_name,
    raw_data:"brand"::STRING         AS brand,
    raw_data:"categories"::STRING    AS category,
    raw_data:"sub_category"::STRING  AS sub_category,
    raw_data:"upc"::STRING           AS upc,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_PRODUCTS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
