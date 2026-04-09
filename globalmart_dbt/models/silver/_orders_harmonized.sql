{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_orders') }}
),

harmonized AS (
    SELECT
        order_id,
        customer_id,
        vendor_id,

        -- Map ship mode abbreviations
        CASE
            WHEN TRIM(ship_mode) IN ('1st', '1st Class') THEN 'First Class'
            WHEN TRIM(ship_mode) IN ('2nd', '2nd Class') THEN 'Second Class'
            WHEN TRIM(ship_mode) IN ('Std', 'Std Class') THEN 'Standard Class'
            ELSE TRIM(ship_mode)
        END AS ship_mode,

        -- Normalize order status
        LOWER(TRIM(order_status)) AS order_status,

        -- Save raw date for rejection reporting
        order_purchase_date AS _raw_order_date,

        -- Parse dates with multiple format attempts
        COALESCE(
            TRY_TO_TIMESTAMP(order_purchase_date, 'MM/DD/YYYY HH24:MI'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'YYYY-MM-DD HH24:MI'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'MM/DD/YYYY'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP(order_purchase_date, 'YYYY-MM-DD"T"HH24:MI:SS')
        ) AS order_purchase_date,

        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 5: order_id NOT NULL
        (order_id IS NULL) AS _fail_order_id_null,
        -- Rule 6: customer_id NOT NULL
        (customer_id IS NULL) AS _fail_customer_id_null,
        -- Rule 7: valid ship mode
        (ship_mode IS NULL OR ship_mode NOT IN ('First Class', 'Second Class', 'Standard Class', 'Same Day'))
            AS _fail_invalid_ship_mode,
        -- Rule 8: valid order status
        (order_status IS NULL OR order_status NOT IN
            ('canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable'))
            AS _fail_invalid_order_status,
        -- Rule 9: parseable date
        (order_purchase_date IS NULL AND _raw_order_date IS NOT NULL)
            AS _fail_unparseable_date,

        -- Composite
        (order_id IS NULL)
        OR (customer_id IS NULL)
        OR (ship_mode IS NULL OR ship_mode NOT IN ('First Class', 'Second Class', 'Standard Class', 'Same Day'))
        OR (order_status IS NULL OR order_status NOT IN
            ('canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable'))
        OR (order_purchase_date IS NULL AND _raw_order_date IS NOT NULL)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
