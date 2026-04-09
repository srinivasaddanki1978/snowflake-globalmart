{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"order_id"::STRING            AS order_id,
    raw_data:"customer_id"::STRING         AS customer_id,
    raw_data:"vendor_id"::STRING           AS vendor_id,
    raw_data:"ship_mode"::STRING           AS ship_mode,
    raw_data:"order_status"::STRING        AS order_status,
    raw_data:"order_purchase_date"::STRING AS order_purchase_date,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_ORDERS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
