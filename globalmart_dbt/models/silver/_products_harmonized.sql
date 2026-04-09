{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_products') }}
),

harmonized AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(brand) AS brand,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        -- Fix UPC scientific notation: 1.23456E+11 → "123456000000"
        CAST(CAST(TRY_TO_DOUBLE(upc) AS BIGINT) AS VARCHAR) AS upc,
        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 20: product_id NOT NULL
        (product_id IS NULL) AS _fail_product_id_null,
        -- Rule 21: product_name NOT NULL
        (product_name IS NULL) AS _fail_product_name_null,

        -- Composite
        (product_id IS NULL) OR (product_name IS NULL)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
