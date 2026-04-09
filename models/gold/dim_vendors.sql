{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id
            ORDER BY _load_timestamp DESC
        ) AS row_num
    FROM {{ ref('clean_vendors') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} AS vendor_sk,
    vendor_id,
    vendor_name
FROM ranked
WHERE row_num = 1
