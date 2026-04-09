{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_vendors') }}
),

harmonized AS (
    SELECT
        TRIM(vendor_id) AS vendor_id,
        TRIM(vendor_name) AS vendor_name,
        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 22: vendor_id NOT NULL
        (vendor_id IS NULL OR vendor_id = '') AS _fail_vendor_id_null,
        -- Rule 23: vendor_name NOT NULL
        (vendor_name IS NULL OR vendor_name = '') AS _fail_vendor_name_null,

        -- Composite
        (vendor_id IS NULL OR vendor_id = '')
        OR (vendor_name IS NULL OR vendor_name = '')
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
