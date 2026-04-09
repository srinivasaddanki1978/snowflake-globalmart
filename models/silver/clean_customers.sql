{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    customer_id,
    customer_name,
    customer_email,
    segment,
    city,
    state,
    region,
    _source_file,
    _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE NOT _has_any_failure
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
