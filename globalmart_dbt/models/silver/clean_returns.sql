{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    order_id,
    return_reason,
    return_status,
    refund_amount,
    return_date,
    _source_file,
    _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE NOT _has_any_failure
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
