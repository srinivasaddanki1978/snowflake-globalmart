{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_transactions') }}
),

harmonized AS (
    SELECT
        -- COALESCE order_id variants
        COALESCE(order_id, order_id_v1, order_id_v2) AS order_id,

        -- COALESCE product_id variants
        COALESCE(product_id, product_id_v1, product_id_v2) AS product_id,

        vendor_id,

        -- Save raw values for rejection reporting
        sales AS _raw_sales,
        discount AS _raw_discount,

        -- Strip currency symbols from sales
        TRY_TO_DOUBLE(REGEXP_REPLACE(sales, '[^0-9.\\-]', '')) AS sales,

        -- Cast quantity
        TRY_TO_NUMBER(quantity) AS quantity,

        -- Convert percentage discounts
        CASE
            WHEN discount LIKE '%\\%%'
                THEN TRY_TO_DOUBLE(REPLACE(discount, '%', '')) / 100.0
            ELSE TRY_TO_DOUBLE(discount)
        END AS discount,

        TRY_TO_DOUBLE(profit) AS profit,
        payment_type,
        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 10: order_id NOT NULL
        (order_id IS NULL) AS _fail_order_id_null,
        -- Rule 11: product_id NOT NULL
        (product_id IS NULL) AS _fail_product_id_null,
        -- Rule 12: non-negative sales
        (sales IS NULL OR sales < 0) AS _fail_negative_sales,
        -- Rule 13: positive quantity
        (quantity IS NULL OR quantity <= 0) AS _fail_non_positive_quantity,
        -- Rule 14: discount in range 0-1
        (discount IS NULL OR discount < 0 OR discount > 1) AS _fail_discount_out_of_range,

        -- Composite
        (order_id IS NULL)
        OR (product_id IS NULL)
        OR (sales IS NULL OR sales < 0)
        OR (quantity IS NULL OR quantity <= 0)
        OR (discount IS NULL OR discount < 0 OR discount > 1)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
