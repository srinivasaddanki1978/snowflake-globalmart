{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"order_id"::STRING       AS order_id_v1,
    raw_data:"OrderId"::STRING        AS order_id_v2,
    raw_data:"return_reason"::STRING  AS return_reason_v1,
    raw_data:"reason"::STRING         AS return_reason_v2,
    raw_data:"return_status"::STRING  AS return_status_v1,
    raw_data:"status"::STRING         AS return_status_v2,
    raw_data:"refund_amount"::STRING  AS refund_amount_v1,
    raw_data:"RefundAmount"::STRING   AS refund_amount_v2,
    raw_data:"amount"::STRING         AS refund_amount_v3,
    raw_data:"return_date"::STRING    AS return_date_v1,
    raw_data:"ReturnDate"::STRING     AS return_date_v2,
    raw_data:"date_of_return"::STRING AS return_date_v3,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_RETURNS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
