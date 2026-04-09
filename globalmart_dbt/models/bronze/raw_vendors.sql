{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"vendor_id"::STRING   AS vendor_id,
    raw_data:"vendor_name"::STRING AS vendor_name,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_VENDORS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
