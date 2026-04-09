{{ config(materialized='table') }}

-- GROUP BY vendor_id guarantees one row per vendor
SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} AS vendor_sk,
    vendor_id,
    MAX(vendor_name) AS vendor_name
FROM {{ ref('clean_vendors') }}
WHERE vendor_id IS NOT NULL
GROUP BY vendor_id
