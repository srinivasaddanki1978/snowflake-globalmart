{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                _load_timestamp DESC,
                CASE WHEN customer_email IS NOT NULL THEN 0 ELSE 1 END
        ) AS row_num
    FROM {{ ref('clean_customers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
    customer_id,
    customer_name,
    customer_email,
    segment,
    city,
    state,
    region
FROM ranked
WHERE row_num = 1
