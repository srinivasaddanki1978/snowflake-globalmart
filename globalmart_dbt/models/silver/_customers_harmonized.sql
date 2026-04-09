{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_customers') }}
),

harmonized AS (
    SELECT
        -- COALESCE 4 ID column variants
        COALESCE(customer_id, customerid, cust_id, customer_identifier) AS customer_id,

        -- COALESCE name variants
        COALESCE(customer_name, full_name) AS customer_name,

        -- COALESCE email variants
        COALESCE(customer_email, email_address) AS customer_email,

        -- COALESCE + map segment
        CASE UPPER(TRIM(COALESCE(segment, customer_segment)))
            WHEN 'CONS' THEN 'Consumer'
            WHEN 'CORP' THEN 'Corporate'
            WHEN 'HO' THEN 'Home Office'
            WHEN 'COSUMER' THEN 'Consumer'
            WHEN 'CONSUMER' THEN 'Consumer'
            WHEN 'CORPORATE' THEN 'Corporate'
            WHEN 'HOME OFFICE' THEN 'Home Office'
            ELSE TRIM(COALESCE(segment, customer_segment))
        END AS segment,

        -- City/state swap for Region 4
        CASE
            WHEN _source_file ILIKE '%Region%4%' THEN state
            ELSE city
        END AS city,
        CASE
            WHEN _source_file ILIKE '%Region%4%' THEN city
            ELSE state
        END AS state,

        -- Map region abbreviations
        CASE UPPER(TRIM(region))
            WHEN 'W' THEN 'West'
            WHEN 'S' THEN 'South'
            WHEN 'EAST' THEN 'East'
            WHEN 'WEST' THEN 'West'
            WHEN 'SOUTH' THEN 'South'
            WHEN 'CENTRAL' THEN 'Central'
            WHEN 'NORTH' THEN 'North'
            ELSE TRIM(region)
        END AS region,

        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 1: customer_id NOT NULL
        (customer_id IS NULL) AS _fail_customer_id_null,
        -- Rule 2: customer_name NOT NULL
        (customer_name IS NULL) AS _fail_customer_name_null,
        -- Rule 3: valid segment
        (segment IS NULL OR segment NOT IN ('Consumer', 'Corporate', 'Home Office'))
            AS _fail_invalid_segment,
        -- Rule 4: valid region
        (region IS NULL OR region NOT IN ('East', 'West', 'South', 'Central', 'North'))
            AS _fail_invalid_region,

        -- Composite failure flag
        (customer_id IS NULL)
        OR (customer_name IS NULL)
        OR (segment IS NULL OR segment NOT IN ('Consumer', 'Corporate', 'Home Office'))
        OR (region IS NULL OR region NOT IN ('East', 'West', 'South', 'Central', 'North'))
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
