{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"order_id"::STRING    AS order_id,
    raw_data:"Order_id"::STRING    AS order_id_v1,
    raw_data:"Order_ID"::STRING    AS order_id_v2,
    raw_data:"product_id"::STRING  AS product_id,
    raw_data:"Product_id"::STRING  AS product_id_v1,
    raw_data:"Product_ID"::STRING  AS product_id_v2,
    raw_data:"vendor_id"::STRING   AS vendor_id,
    raw_data:"Sales"::STRING       AS sales,
    raw_data:"Quantity"::STRING    AS quantity,
    raw_data:"discount"::STRING    AS discount,
    raw_data:"profit"::STRING      AS profit,
    raw_data:"payment_type"::STRING AS payment_type,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_TRANSACTIONS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
