{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} AS vendor_sk,
    vendor_id,
    vendor_name
FROM {{ ref('clean_vendors') }}
