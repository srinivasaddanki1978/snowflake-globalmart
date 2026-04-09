{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    vendor_id,
    vendor_name,
    _source_file,
    _load_timestamp
FROM {{ ref('_vendors_harmonized') }}
WHERE NOT _has_any_failure
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
