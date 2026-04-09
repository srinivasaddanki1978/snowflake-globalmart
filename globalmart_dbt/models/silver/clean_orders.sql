{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    order_id,
    customer_id,
    vendor_id,
    ship_mode,
    order_status,
    order_purchase_date,
    _source_file,
    _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE NOT _has_any_failure
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
