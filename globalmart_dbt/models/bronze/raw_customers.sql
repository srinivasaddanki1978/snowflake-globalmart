{{ config(materialized='incremental', incremental_strategy='append') }}

SELECT
    raw_data:"customer_id"::STRING        AS customer_id,
    raw_data:"CustomerID"::STRING         AS customerid,
    raw_data:"cust_id"::STRING            AS cust_id,
    raw_data:"customer_identifier"::STRING AS customer_identifier,
    raw_data:"customer_name"::STRING      AS customer_name,
    raw_data:"full_name"::STRING          AS full_name,
    raw_data:"customer_email"::STRING     AS customer_email,
    raw_data:"email_address"::STRING      AS email_address,
    raw_data:"segment"::STRING            AS segment,
    raw_data:"customer_segment"::STRING   AS customer_segment,
    raw_data:"city"::STRING               AS city,
    raw_data:"state"::STRING              AS state,
    raw_data:"region"::STRING             AS region,
    _source_file,
    _load_timestamp
FROM {{ source('raw', 'RAW_CUSTOMERS_LOAD') }}
{% if is_incremental() %}
  WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
