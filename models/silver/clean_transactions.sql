{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    order_id,
    product_id,
    vendor_id,
    sales,
    quantity,
    discount,
    profit,
    payment_type,
    _source_file,
    _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE NOT _has_any_failure
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
